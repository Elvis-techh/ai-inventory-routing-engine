import concurrent.futures
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import firebase_admin
from firebase_admin import firestore
from flask import Flask, jsonify, request
from google.api_core import retry as api_retry
from google.api_core.exceptions import (
    BadRequest,
    DeadlineExceeded,
    Forbidden,
    GoogleAPICallError,
    InternalServerError,
    NotFound,
    RetryError,
    ServiceUnavailable,
    TooManyRequests,
)
from google.cloud import bigquery
from google.cloud.firestore_v1 import FieldFilter

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

LOCKS_ENABLED = os.getenv("LOCKS_ENABLED", "true").lower() in ("1", "true", "yes", "on")
FIRESTORE_DEADLINE_S = float(os.getenv("FIRESTORE_DEADLINE_S", "5"))
FIRESTORE_RETRY = api_retry.Retry(deadline=FIRESTORE_DEADLINE_S)


def init_firebase():
    try:
        firebase_admin.get_app()
        return
    except ValueError:
        pass

    project_id = (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCLOUD_PROJECT")
        or os.getenv("FIREBASE_PROJECT_ID")
    )

    if project_id:
        firebase_admin.initialize_app(options={"projectId": project_id})
    else:
        firebase_admin.initialize_app()


init_firebase()
fs = firestore.client()


def utcnow():
    return datetime.now(timezone.utc)


def hu_doc_id(hu_id: str) -> str:
    return f"{hu_id}"


def aisle_doc_id(zone: str, aisle: int) -> str:
    return f"{zone}:{aisle}"


def _to_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class LockManager:
    def __init__(self, db, *, retry=None, timeout: Optional[float] = None):
        self.db = db
        self.retry = retry
        self.timeout = timeout

    def get_sticky_payload(self, hu_id: str, owner_id: str | None = None):
        now = utcnow()
        hu_ref = self.db.collection("hu_reservations").document(hu_doc_id(hu_id))
        hu_snap = hu_ref.get(timeout=self.timeout, retry=self.retry)

        if not hu_snap.exists:
            return None

        hu_doc = hu_snap.to_dict() or {}
        if not self._is_active(hu_doc, now):
            return None

        if owner_id and hu_doc.get("ownerId") and hu_doc.get("ownerId") != owner_id:
            return None

        return hu_doc.get("payload")

    def reserve_aisle(
        self,
        *,
        wh: int,
        owner_id: str,
        hu_id: str,
        zone: str,
        aisle: int,
        payload,
        ttl_min: int = 7,
    ):
        now = utcnow()
        exp = now + timedelta(minutes=ttl_min)
        lease_key = aisle_doc_id(zone, aisle)

        lease_ref = self.db.collection("aisle_leases").document(lease_key)
        hu_ref = self.db.collection("hu_reservations").document(hu_doc_id(hu_id))

        txn = self.db.transaction()

        @firestore.transactional
        def _tx(transaction):
            snap = lease_ref.get(
                transaction=transaction, timeout=self.timeout, retry=self.retry
            )
            if snap.exists:
                d = snap.to_dict() or {}
                current_hu = d.get("ownerHuId") or d.get("lastHuId")

                if self._is_active(d, now):
                    hu_mismatch = current_hu != hu_id
                    current_owner = d.get("ownerId")
                    owner_mismatch = current_owner and current_owner != owner_id

                    if not current_hu or hu_mismatch or owner_mismatch:
                        return {
                            "ok": False,
                            "reason": "aisle_locked",
                            "lockedByHuId": current_hu,
                            "lockedByOwnerId": current_owner,
                            "expiresAt": d.get("expiresAt"),
                        }

            transaction.set(
                lease_ref,
                {
                    "wh": wh,
                    "zone": zone,
                    "aisle": aisle,
                    "ownerHuId": hu_id,
                    "ownerId": owner_id,
                    "lastHuId": hu_id,
                    "expiresAt": exp,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )

            transaction.set(
                hu_ref,
                {
                    "wh": wh,
                    "huId": hu_id,
                    "ownerId": owner_id,
                    "aisleKey": lease_key,
                    "payload": payload,
                    "expiresAt": exp,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )

            return {
                "ok": True,
                "payload": payload,
                "lease": {"aisleKey": lease_key, "expiresAt": exp},
            }

        return _tx(txn)

    def reserve_location(
        self,
        *,
        wh: int,
        owner_id: str,
        hu_id: str,
        location_id: str,
        ttl_min: int = 11,
        status: str = "RESERVED",
    ):
        now = utcnow()
        exp = now + timedelta(minutes=ttl_min)
        loc_ref = self.db.collection("location_leases").document(str(location_id))
        txn = self.db.transaction()

        @firestore.transactional
        def _tx(transaction):
            snap = loc_ref.get(
                transaction=transaction, timeout=self.timeout, retry=self.retry
            )
            if snap.exists:
                d = snap.to_dict() or {}
                if self._is_active(d, now):
                    current_hu = d.get("ownerHuId") or d.get("lastHuId")
                    if current_hu and current_hu != hu_id:
                        return {
                            "ok": False,
                            "reason": "location_locked",
                            "lockedByHuId": current_hu,
                            "status": d.get("status", "unknown"),
                            "expiresAt": d.get("expiresAt"),
                        }

            transaction.set(
                loc_ref,
                {
                    "wh": wh,
                    "location_id": str(location_id),
                    "ownerHuId": hu_id,
                    "ownerId": owner_id,
                    "lastHuId": hu_id,
                    "status": status,
                    "expiresAt": exp,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            return {"ok": True, "expiresAt": exp}

        return _tx(txn)

    def _is_active(self, doc: dict, now: datetime) -> bool:
        exp = _to_utc(doc.get("expiresAt"))
        return exp is not None and exp > now


locks = LockManager(fs, retry=FIRESTORE_RETRY, timeout=FIRESTORE_DEADLINE_S)
PUBLIC_DIR = Path(__file__).resolve().parents[1] / "public"
app = Flask(__name__, static_folder=str(PUBLIC_DIR), static_url_path="")

try:
    bq_client = bigquery.Client()
except Exception as e:
    logging.warning(f"Could not initialize BigQuery client locally: {e}")
    bq_client = None

# ------------------------------------------------------------------------------
# SQL Query (Optimized: Adapters, Simplified Logic, Removed Dock Hints)
# ------------------------------------------------------------------------------
QUERY = r"""
DECLARE v_mode STRING DEFAULT UPPER(@mode);
DECLARE v_dock_override STRING DEFAULT NULLIF(@dock_override, '');

DECLARE v_excluded_aisles ARRAY<STRING> DEFAULT @excluded_aisles;
DECLARE v_excluded_locations ARRAY<STRING> DEFAULT @excluded_locations;

DECLARE v_hu_id STRING DEFAULT @hu_id;
DECLARE v_wh_id_i INT64 DEFAULT 999; -- Sanitized Warehouse ID
DECLARE v_qty INT64 DEFAULT @qty;

WITH settings AS (
  SELECT 10 AS top_k_aisles, 3 AS suggestions_per_run
),

-- [ADAPTER] Maps XY_EmptyLocations to standard naming convention
capman_adapter AS (
  SELECT
    location_id,
    wh_id, 
    LB_zone AS zone,
    SAFE_CAST(LB_aisle AS INT64) AS aisle_i,
    SAFE_CAST(LB_bay AS INT64) AS bay_i,
    SAFE_CAST(LB_level AS INT64) AS level_i,
    LB_position AS position,
    description AS rack_type,
    
    SAFE_CAST(length AS FLOAT64) AS depth_in,
    SAFE_CAST(width AS FLOAT64) AS width_in,
    SAFE_CAST(height AS FLOAT64) AS height_in,
    
    SAFE_CAST(capacity_volume AS FLOAT64) AS capacity_cuft
  FROM `your-gcp-project-ops.capacity_management.XY_EmptyLocations`
  WHERE SAFE_CAST(wh_id AS INT64) = v_wh_id_i
),

hu_pick AS (
  SELECT DISTINCT
    CAST(hd.hu_id AS STRING) AS hu_id,
    CAST(hd.item_number AS STRING) AS sku,
    CAST(hd.item_id AS INT64) AS item_id,
    CAST(hd.location_id AS STRING) AS current_location
  FROM `your-gcp-project-data.schema.t_hu_detail` hd
  WHERE SAFE_CAST(hd.wh_id AS INT64) = v_wh_id_i
    AND CAST(hd.hu_id AS STRING) = v_hu_id
),

dims AS (
  SELECT
    x.ItemID,
    x.L, x.W, x.H, x.Weight,
    (x.L * x.W * x.H) / 1728.0 AS unit_cube_ft,
    GREATEST(x.L, x.W, x.H) AS max_dim,
    LEAST(x.L, x.W, x.H) AS min_dim,
    (x.L + x.W + x.H) - GREATEST(x.L, x.W, x.H) - LEAST(x.L, x.W, x.H) AS mid_dim
  FROM (
    SELECT
      ib.ItemID,
      SAFE_CAST(ib.length AS FLOAT64) AS L,
      SAFE_CAST(ib.width  AS FLOAT64) AS W,
      SAFE_CAST(ib.height AS FLOAT64) AS H,
      SAFE_CAST(ib.weight AS FLOAT64) AS Weight
    FROM `your-gcp-project-data.schema.tbl_item_box` ib
    JOIN hu_pick hp ON hp.item_id = CAST(ib.ItemID AS INT64)
  ) x
  WHERE x.L > 0 AND x.W > 0 AND x.H > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY x.ItemID ORDER BY (x.L * x.W * x.H) DESC) = 1
),

policy_all AS (
  SELECT
    hp.hu_id, hp.sku, hp.item_id, hp.current_location,
    d.L, d.W, d.H, d.Weight, d.unit_cube_ft,
    d.max_dim, d.mid_dim, d.min_dim
  FROM hu_pick hp
  JOIN dims d ON d.ItemID = hp.item_id
),

policy_one AS (
  SELECT
    hu_id,
    sku,
    item_id,
    current_location,

    dims.L, dims.W, dims.H, dims.Weight,
    unit_cube_ft,
    unit_cube_ft * v_qty AS total_cube_ft,

    dims.max_dim, dims.mid_dim, dims.min_dim,

    CASE
      WHEN (
        dims.max_dim <= 87 
        AND dims.mid_dim <= 42 
        AND dims.min_dim <= 12
        AND (unit_cube_ft * v_qty) <= 20.0 -- Sanitized dimension
      ) THEN 'FLAT'
      ELSE
        CASE
          WHEN dims.max_dim > 70 THEN 'HRD_XL'
          WHEN dims.max_dim > 60 THEN 'HRD_L'
          WHEN dims.max_dim > 36 THEN 'HRD_M'
          ELSE 'HRD_S'
        END
    END AS target_rack_bucket

  FROM (
    SELECT
      ANY_VALUE(hu_id) AS hu_id,
      ANY_VALUE(current_location) AS current_location,
      ANY_VALUE(sku) AS sku,
      ANY_VALUE(item_id) AS item_id,
      MAX(unit_cube_ft) AS unit_cube_ft,
      ARRAY_AGG(STRUCT(L, W, H, Weight, unit_cube_ft, max_dim, mid_dim, min_dim) ORDER BY unit_cube_ft DESC LIMIT 1)[OFFSET(0)] AS dims
    FROM policy_all
  )
),

occupied_locations AS (
  SELECT DISTINCT location_id
  FROM `your-gcp-project-bulk.schema.t_stored_item`
  WHERE SAFE_CAST(wh_id AS INT64) = v_wh_id_i
    AND actual_qty > 0
),

excluded_locations AS (
  SELECT DISTINCT loc AS location_id
  FROM UNNEST(v_excluded_locations) AS loc
  WHERE loc IS NOT NULL
),

locs_raw AS (
  SELECT
    r.location_id,
    r.zone,
    r.aisle_i,
    r.bay_i,
    r.level_i,
    r.position,
    r.rack_type,

    IF(ol.location_id IS NOT NULL, 1000.0, 0.0) AS used_cuft, 

    CASE
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 18 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 100.0 -- Sanitized dimension
          WHEN r.level_i = 3       THEN 100.0
          ELSE r.depth_in 
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 19 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 60.0 -- Sanitized dimension
          WHEN r.level_i = 3       THEN 60.0
          ELSE r.depth_in 
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 20 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 100.0
          WHEN r.level_i = 3       THEN 100.0
          ELSE r.depth_in 
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (79, 80) THEN 80.0
      ELSE r.depth_in 
    END AS depth_in,

    CASE
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (18, 19, 20) THEN 25.0
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (79, 80) THEN 40.0
      ELSE r.width_in 
    END AS width_in,

    CASE
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (18, 19, 20) THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 15.0
          WHEN r.level_i = 3       THEN 10.0
          ELSE r.height_in
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (79, 80) THEN 12.0
      ELSE r.height_in 
    END AS height_in,

    CASE
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 18 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 50.0
          WHEN r.level_i = 3       THEN 30.0
          ELSE r.capacity_cuft
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 19 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 30.0
          WHEN r.level_i = 3       THEN 15.0
          ELSE r.capacity_cuft
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i = 20 THEN
        CASE 
          WHEN r.level_i IN (1, 2) THEN 50.0
          WHEN r.level_i = 3       THEN 30.0
          ELSE r.capacity_cuft
        END
      WHEN p.target_rack_bucket = 'FLAT' AND r.zone = 'P' AND r.aisle_i IN (79, 80) THEN 20.0
      ELSE r.capacity_cuft
    END AS capacity_cuft

  FROM capman_adapter r 
  CROSS JOIN policy_one p 
  LEFT JOIN occupied_locations ol ON r.location_id = ol.location_id
  
  WHERE 
    (
       (p.target_rack_bucket = 'FLAT' AND (
         (r.zone='P' AND r.aisle_i BETWEEN 18 AND 20 AND r.level_i BETWEEN 1 AND 3)
         OR
         (r.zone='P' AND r.aisle_i BETWEEN 79 AND 80 AND r.level_i BETWEEN 1 AND 6)
       )
       AND (
          (r.aisle_i = 18 AND MOD(r.bay_i, 2) = 1)
          OR (r.aisle_i = 19)
          OR (r.aisle_i = 20 AND MOD(r.bay_i, 2) = 0)
          OR (r.aisle_i = 79 AND MOD(r.bay_i, 2) = 1)
          OR (r.aisle_i = 80 AND MOD(r.bay_i, 2) = 0)
       ))
       OR
       (p.target_rack_bucket != 'FLAT' AND v_mode='OP' AND (
         (r.zone='P' AND r.aisle_i BETWEEN 23 AND 77 AND NOT (r.aisle_i BETWEEN 68 AND 77))
         OR (r.zone='V' AND r.aisle_i BETWEEN 39 AND 43)
         OR (r.zone='T' AND r.aisle_i BETWEEN 13 AND 20)
       ))
       OR
       (p.target_rack_bucket != 'FLAT' AND v_mode='REACH' AND (
         (r.zone='P' AND r.aisle_i BETWEEN 14 AND 20)
         OR (r.zone='P' AND r.aisle_i BETWEEN 79 AND 91)
         OR (r.zone='V' AND r.aisle_i BETWEEN 34 AND 38)
       ))
    )
    AND (
       (v_mode = 'REACH' AND ol.location_id IS NULL)
       OR
       (v_mode = 'OP' AND ol.location_id IS NULL) 
    )
    AND r.location_id NOT IN (SELECT location_id FROM excluded_locations)
),

locs_mapped AS (
  SELECT
    lr.*,
    (lr.capacity_cuft - lr.used_cuft) AS remaining_cuft,
    
    CASE
      WHEN lr.zone = 'T' AND lr.aisle_i BETWEEN 13 AND 20 THEN
        CASE WHEN lr.level_i = 1 THEN 'HRD_L' WHEN lr.level_i BETWEEN 2 AND 7 THEN 'HRD_S' END
      WHEN lr.zone = 'V' AND lr.aisle_i BETWEEN 39 AND 43 THEN
        CASE WHEN lr.level_i = 1 THEN 'HRD_L' WHEN lr.level_i BETWEEN 2 AND 7 THEN 'HRD_M' END
      WHEN lr.aisle_i BETWEEN 23 AND 64 THEN
        CASE WHEN lr.level_i BETWEEN 1 AND 2 THEN 'HRD_XL' WHEN lr.level_i BETWEEN 3 AND 4 THEN 'HRD_L' WHEN lr.level_i = 5 THEN 'HRD_S' END
      WHEN lr.aisle_i BETWEEN 65 AND 69 THEN
        CASE WHEN lr.level_i BETWEEN 1 AND 2 THEN 'HRD_L' WHEN lr.level_i BETWEEN 3 AND 6 THEN 'HRD_M' WHEN lr.level_i = 7 THEN 'HRD_S' END
    END AS mapped_rack_type
  FROM locs_raw lr
),

locs_scored AS (
  SELECT
    lm.location_id,
    lm.zone,
    lm.aisle_i,
    lm.bay_i,
    lm.level_i,
    lm.position,
    lm.remaining_cuft,
    lm.depth_in,
    lm.width_in,
    lm.height_in,
    COALESCE(lm.mapped_rack_type, lm.rack_type) AS effective_rack_type,
    p.hu_id,
    p.sku,
    p.item_id,
    p.current_location,
    p.L, p.W, p.H, p.Weight,
    p.unit_cube_ft,
    p.total_cube_ft,
    p.target_rack_bucket,
    
    IF(p.target_rack_bucket = 'FLAT', 'FLAT', COALESCE(lm.mapped_rack_type, lm.rack_type)) AS candidate_bucket,
    
    CASE
      WHEN v_mode = 'REACH' THEN 1
      ELSE COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.remaining_cuft, p.unit_cube_ft)) AS INT64), 0)
    END AS units_fit_cube,
    
    CASE
      WHEN v_mode = 'REACH' THEN 1
      ELSE 
        CAST(GREATEST(
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.L)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.W)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.H)) AS INT64), 0),
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.L)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.H)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.W)) AS INT64), 0),
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.W)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.L)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.H)) AS INT64), 0),
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.W)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.H)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.L)) AS INT64), 0),
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.H)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.L)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.W)) AS INT64), 0),
            COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.depth_in, p.H)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.width_in, p.W)) AS INT64), 0) * COALESCE(CAST(FLOOR(SAFE_DIVIDE(lm.height_in, p.L)) AS INT64), 0)
        ) AS INT64)
    END AS units_fit_dim
  FROM locs_mapped lm
  CROSS JOIN policy_one p
),

locs_ok AS (
  SELECT
    *,
    LEAST(units_fit_cube, units_fit_dim) AS units_fit
  FROM locs_scored
  WHERE 
    ((v_mode = 'OP' AND candidate_bucket = target_rack_bucket) OR (v_mode = 'REACH'))
    AND units_fit_cube >= 1
    AND units_fit_dim >= 1
),

per_aisle AS (
  SELECT
    candidate_bucket AS rack_bucket,
    zone,
    aisle_i AS aisle,
    SUM(remaining_cuft) AS total_remaining,
    COUNT(*) AS loc_count
  FROM locs_ok
  CROSS JOIN policy_one p
  WHERE CONCAT(zone, ':', CAST(aisle_i AS STRING)) NOT IN UNNEST(v_excluded_aisles)
  GROUP BY rack_bucket, zone, aisle
),

aisle_prefs AS (
  SELECT
    pa.*,
    CASE
      WHEN p.target_rack_bucket = 'FLAT' THEN 0
      WHEN v_dock_override ='DOCK_A' THEN
        CASE WHEN pa.zone='V' THEN 0 WHEN pa.zone='P' THEN 1 WHEN pa.zone='T' THEN 2 ELSE 9 END
      WHEN v_dock_override ='DOCK_B' THEN
        CASE WHEN pa.zone='T' THEN 0 WHEN pa.zone='P' THEN 1 WHEN pa.zone='V' THEN 2 ELSE 9 END
      ELSE 5
    END AS pref_zone,

    CASE
      WHEN p.target_rack_bucket = 'FLAT' THEN
        CASE
          WHEN v_dock_override='DOCK_A' THEN
            CASE
              WHEN pa.zone='P' AND pa.aisle BETWEEN 18 AND 19 THEN pa.aisle - 18
              WHEN pa.zone='P' AND pa.aisle BETWEEN 79 AND 80 THEN (pa.aisle - 79) + 100
              ELSE 999
            END
          WHEN v_dock_override='DOCK_B' THEN
            CASE
              WHEN pa.zone='P' AND pa.aisle BETWEEN 79 AND 80 THEN 80 - pa.aisle
              WHEN pa.zone='P' AND pa.aisle BETWEEN 18 AND 19 THEN (19 - pa.aisle) + 100
              ELSE 999
            END
          ELSE 500
        END
      WHEN v_mode='REACH' THEN
        CASE
          WHEN v_dock_override='DOCK_A' THEN
            CASE
              WHEN pa.zone='V' AND pa.aisle BETWEEN 34 AND 38 THEN pa.aisle - 34
              WHEN pa.zone='P' AND pa.aisle BETWEEN 14 AND 20 THEN pa.aisle - 14
              WHEN pa.zone='P' AND pa.aisle BETWEEN 79 AND 91 THEN (pa.aisle - 79) + 100
              ELSE 999
            END
          WHEN v_dock_override='DOCK_B' THEN
            CASE
              WHEN pa.zone='P' AND pa.aisle BETWEEN 79 AND 91 THEN 91 - pa.aisle
              WHEN pa.zone='P' AND pa.aisle BETWEEN 14 AND 20 THEN (20 - pa.aisle) + 100
              WHEN pa.zone='V' AND pa.aisle BETWEEN 34 AND 38 THEN 38 - pa.aisle
              ELSE 999
            END
          ELSE 500
        END
      ELSE
        CASE
          WHEN v_dock_override='DOCK_A' THEN
            CASE WHEN pa.zone='V' THEN pa.aisle - 39
                 WHEN pa.zone='P' THEN pa.aisle - 23
                 WHEN pa.zone='T' THEN pa.aisle - 13
                 ELSE 999 END
          WHEN v_dock_override='DOCK_B' THEN
            CASE WHEN pa.zone='T' THEN 20 - pa.aisle
                 WHEN pa.zone='P' THEN 77 - pa.aisle
                 WHEN pa.zone='V' THEN 43 - pa.aisle
                 ELSE 999 END
          ELSE 500
        END
    END AS pref_within
  FROM per_aisle pa
  CROSS JOIN policy_one p
),

top_aisle_candidates AS (
  SELECT * FROM aisle_prefs
  QUALIFY ROW_NUMBER() OVER (ORDER BY pref_zone ASC, pref_within ASC, total_remaining DESC) <= (SELECT top_k_aisles FROM settings)
),

candidates_ranked AS (
  SELECT
    tac.zone,
    tac.aisle,
    tac.rack_bucket,
    tac.total_remaining,
    tac.loc_count,
    IF(tac.loc_count >= (SELECT suggestions_per_run FROM settings), 0, 1) AS insufficient,
    ROW_NUMBER() OVER (
      ORDER BY
        IF(tac.loc_count >= (SELECT suggestions_per_run FROM settings), 0, 1) ASC,
        tac.pref_zone ASC,
        tac.pref_within ASC,
        tac.total_remaining DESC
    ) AS aisle_rank
  FROM top_aisle_candidates tac
),

candidate_locs AS (
  SELECT l.*, cr.aisle_rank
  FROM locs_ok l
  JOIN candidates_ranked cr ON l.zone = cr.zone AND l.aisle_i = cr.aisle
),

ranked_locs AS (
  SELECT
    aisle_rank, zone, aisle_i AS aisle, location_id, level_i, bay_i, position, remaining_cuft, units_fit,
    ROW_NUMBER() OVER (
      PARTITION BY aisle_rank
      ORDER BY units_fit DESC, remaining_cuft DESC, bay_i DESC, level_i DESC, position DESC
    ) AS rn
  FROM candidate_locs
),

alloc AS (
  SELECT rl.*, SUM(units_fit) OVER (PARTITION BY aisle_rank ORDER BY rn) AS cum_units
  FROM ranked_locs rl
),

alloc2 AS (
  SELECT a.*, LAG(cum_units, 1, 0) OVER (PARTITION BY aisle_rank ORDER BY rn) AS cum_before
  FROM alloc a
),

picked_locs AS (
  SELECT
    aisle_rank, zone, aisle, location_id, rn, units_fit, cum_units, cum_before,
    CASE
      WHEN cum_before >= v_qty THEN 0
      WHEN cum_units >= v_qty THEN v_qty - cum_before
      ELSE units_fit
    END AS units_alloc
  FROM alloc2
  WHERE rn <= (SELECT suggestions_per_run FROM settings) AND units_fit > 0
),

aisle_candidates_out AS (
  SELECT
    cr.aisle_rank AS rank, cr.zone, cr.aisle,
    ARRAY_AGG(IF(pl.location_id IS NULL, NULL, STRUCT(pl.location_id, pl.units_alloc AS units, pl.rn)) IGNORE NULLS ORDER BY pl.rn) AS allocations
  FROM candidates_ranked cr
  LEFT JOIN picked_locs pl ON pl.aisle_rank = cr.aisle_rank
  GROUP BY cr.aisle_rank, cr.zone, cr.aisle
)

SELECT
  p.hu_id,
  p.item_id,
  p.sku AS sku,
  p.current_location,
  v_mode AS mode,
  
  TRUE AS current_location_valid,
  TRUE AS use_aisle_locks,
  FALSE AS reach_constraints_active, 

  (p.target_rack_bucket = 'FLAT') AS is_flat,

  p.target_rack_bucket AS policy_rack_type, 
  p.target_rack_bucket AS rack_type,

  v_qty AS units_requested,

  NULL AS dockArea,
  v_dock_override AS dock_class,

  NULL AS sku_home_location,

  ARRAY(
    SELECT AS STRUCT
      ao.zone, ao.aisle, ao.rank, ao.allocations
    FROM aisle_candidates_out ao
    WHERE ARRAY_LENGTH(ao.allocations) > 0
    ORDER BY ao.rank
  ) AS aisle_candidates

FROM policy_one p;
"""


def get_active_excluded_locations(
    wh_id: int, ignore_hu_id: str | None = None
) -> list[str]:
    logging.info("excluded_locations: start")
    now = utcnow()
    q = (
        fs.collection("location_leases")
        .where(filter=FieldFilter("wh", "==", wh_id))
        .where(filter=FieldFilter("expiresAt", ">", now))
    )

    out: list[str] = []
    for snap in q.stream(timeout=FIRESTORE_DEADLINE_S, retry=FIRESTORE_RETRY):
        d = snap.to_dict() or {}
        owner_hu = d.get("ownerHuId") or d.get("lastHuId")
        if ignore_hu_id and owner_hu == ignore_hu_id:
            continue

        loc = d.get("location_id") or d.get("locationId") or snap.id
        if loc:
            out.append(str(loc))

    logging.info("excluded_locations: done n=%d", len(out))
    return out


def get_active_excluded_aisles(
    wh_id: int, ignore_hu_id: str | None = None
) -> list[str]:
    logging.info("excluded_aisles: start")
    now = utcnow()
    q = (
        fs.collection("aisle_leases")
        .where(filter=FieldFilter("wh", "==", wh_id))
        .where(filter=FieldFilter("expiresAt", ">", now))
    )

    out: list[str] = []
    for snap in q.stream(timeout=FIRESTORE_DEADLINE_S, retry=FIRESTORE_RETRY):
        d = snap.to_dict() or {}
        owner_hu = d.get("ownerHuId") or d.get("lastHuId")
        if ignore_hu_id and owner_hu == ignore_hu_id:
            continue

        zone = d.get("zone")
        aisle = d.get("aisle")
        if zone and aisle is not None:
            out.append(f"{zone}:{int(aisle)}")

    logging.info("excluded_aisles: done n=%d", len(out))
    return out


def _row_to_dict(x):
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    try:
        return {k: x[k] for k in x.keys()}
    except Exception:
        try:
            return dict(x)
        except Exception:
            return {"value": x}


def run_query_candidates(
    *,
    hu_id: str,
    qty: int,
    mode: str,
    dock_override: str | None,
    excluded_aisles: list[str],
    excluded_locations: list[str],
) -> dict | None:
    if not bq_client:
        raise RuntimeError("BigQuery client is not initialized.")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mode", "STRING", mode),
            bigquery.ScalarQueryParameter(
                "dock_override", "STRING", dock_override or ""
            ),
            bigquery.ScalarQueryParameter("hu_id", "STRING", hu_id),
            bigquery.ScalarQueryParameter("qty", "INT64", qty),
            bigquery.ArrayQueryParameter(
                "excluded_aisles", "STRING", excluded_aisles or []
            ),
            bigquery.ArrayQueryParameter(
                "excluded_locations", "STRING", excluded_locations or []
            ),
        ]
    )

    job_config.use_legacy_sql = False
    job_config.use_query_cache = True

    try:
        job = bq_client.query(QUERY, job_config=job_config, timeout=22)
    except GoogleAPICallError as e:
        logging.exception("BigQuery query() failed before job creation. err=%r", e)
        raise

    try:
        it = job.result(page_size=1, timeout=22)
    except concurrent.futures.TimeoutError:
        try:
            job.cancel()
        except Exception:
            pass
        raise TimeoutError("BigQuery query timed out")
    except GoogleAPICallError as e:
        try:
            job.reload()
        except Exception:
            pass

        setattr(e, "bq_job_id", getattr(job, "job_id", None))
        setattr(e, "bq_error_result", getattr(job, "error_result", None))
        setattr(e, "bq_errors", getattr(job, "errors", None))

        try:
            job.cancel()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            job.cancel()
        except Exception:
            pass
        raise

    row = next(it, None)
    if row is None:
        return None

    rowd = _row_to_dict(row)

    aisle_candidates = []
    for c in rowd.get("aisle_candidates") or []:
        cd = _row_to_dict(c) or {}
        allocations = []
        for a in cd.get("allocations") or []:
            ad = _row_to_dict(a) or {}
            allocations.append(
                {
                    "location_id": ad.get("location_id"),
                    "units": int(ad.get("units") or 0),
                    "rank": int(ad.get("rn") or 0),
                }
            )

        aisle_candidates.append(
            {
                "zone": cd.get("zone"),
                "aisle": int(cd.get("aisle") or 0),
                "rank": int(cd.get("rank") or 0),
                "allocations": allocations,
            }
        )

    return {
        "hu_id": rowd.get("hu_id"),
        "mode": rowd.get("mode"),
        "dock_class": rowd.get("dock_class"),
        "item_id": rowd.get("item_id"),
        "current_location": rowd.get("current_location"),
        "rack_type": rowd.get("rack_type"),
        "units_requested": rowd.get("units_requested"),
        "current_location_valid": bool(rowd.get("current_location_valid")),
        "use_aisle_locks": bool(rowd.get("use_aisle_locks")),
        "reach_constraints_active": bool(rowd.get("reach_constraints_active")),
        "aisle_candidates": aisle_candidates,
    }


def build_payload_from_candidate(bq_row: dict, candidate: dict):
    allocations = candidate.get("allocations") or []
    location_suggestions = ", ".join(
        [a["location_id"] for a in allocations if a.get("location_id")]
    )

    return [
        {
            "mode": bq_row.get("mode"),
            "dock_class": bq_row.get("dock_class"),
            "current_location": bq_row.get("current_location"),
            "current_location_valid": bq_row.get("current_location_valid"),
            "use_aisle_locks": bq_row.get("use_aisle_locks"),
            "reach_constraints_active": bq_row.get("reach_constraints_active"),
            "rack_type": bq_row.get("rack_type"),
            "location_suggestions": location_suggestions,
            "allocations": allocations,
            "units_requested": bq_row.get("units_requested"),
            "units_covered": sum([a.get("units") or 0 for a in allocations]),
            "hu_id": bq_row.get("hu_id"),
            "selected_aisle": {
                "zone": candidate.get("zone"),
                "aisle": candidate.get("aisle"),
                "rank": candidate.get("rank"),
            },
        }
    ]


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.get("/api/storage")
def storage_lookup():
    logging.info("storage_lookup: start")

    wh_id = 999  # Sanitized Warehouse ID
    hu_id = request.args.get("hu_id")
    qty_raw = request.args.get("qty", "1")
    owner_id = request.args.get("owner_id")

    mode = (request.args.get("mode") or "OP").strip().upper()
    if mode not in ("OP", "REACH"):
        mode = "OP"

    dock = (request.args.get("dock") or "").strip()
    dock_override = dock if dock in ("DOCK_A", "DOCK_B") else None

    if not owner_id:
        return jsonify({"error": "owner_id is required"}), 400

    if not hu_id:
        return jsonify({"error": "hu_id is required"}), 400

    try:
        qty = int(qty_raw)
        if qty <= 0:
            raise ValueError()
    except ValueError:
        return jsonify({"error": "qty must be a positive integer"}), 400

    MAX_QTY = int(os.getenv("MAX_QTY", "99"))
    if qty > MAX_QTY:
        return jsonify({"error": "qty_too_large", "max_qty": MAX_QTY}), 400

    excluded_aisles: list[str] = []
    excluded_locations: list[str] = []

    if LOCKS_ENABLED:
        try:
            sticky = locks.get_sticky_payload(hu_id, owner_id=owner_id)
            if sticky is not None:
                try:
                    meta = sticky[0] if isinstance(sticky, list) and sticky else {}
                    sticky_mode = str(meta.get("mode") or "").upper()
                    sticky_qty = int(meta.get("units_requested") or 0)
                    sticky_dock = str(meta.get("dock_class") or "")

                    if (
                        sticky_mode == mode
                        and sticky_qty == qty
                        and (dock_override is None or dock_override == sticky_dock)
                    ):
                        return jsonify(sticky)
                except Exception:
                    pass
            excluded_aisles = get_active_excluded_aisles(wh_id, ignore_hu_id=hu_id)

            if mode == "REACH":
                excluded_locations = get_active_excluded_locations(
                    wh_id, ignore_hu_id=hu_id
                )

        except (RetryError, DeadlineExceeded, GoogleAPICallError) as e:
            logging.warning(
                "Firestore unavailable (excluded). Continuing without exclusions. err=%r",
                e,
            )
            excluded_aisles = []
            excluded_locations = []

    def _bq_ctx(e: Exception) -> str:
        return f"job_id={getattr(e, 'bq_job_id', None)} error_result={getattr(e, 'bq_error_result', None)} errors={getattr(e, 'bq_errors', None)}"

    try:
        row = run_query_candidates(
            hu_id=hu_id,
            qty=qty,
            mode=mode,
            dock_override=dock_override,
            excluded_aisles=excluded_aisles,
            excluded_locations=excluded_locations,
        )

    except TimeoutError as e:
        logging.warning("BigQuery timeout. %s", _bq_ctx(e))
        return jsonify({"error": "bq_timeout"}), 504
    except BadRequest as e:
        logging.error("BigQuery BadRequest. %s", _bq_ctx(e), exc_info=True)
        return jsonify({"error": "bq_bad_request"}), 400
    except Forbidden as e:
        logging.error("BigQuery Forbidden. %s", _bq_ctx(e), exc_info=True)
        return jsonify({"error": "bq_forbidden"}), 502
    except NotFound as e:
        logging.error("BigQuery NotFound. %s", _bq_ctx(e), exc_info=True)
        return jsonify({"error": "bq_not_found"}), 502
    except (TooManyRequests, ServiceUnavailable, InternalServerError) as e:
        logging.error("BigQuery unavailable. %s", _bq_ctx(e), exc_info=True)
        return jsonify({"error": "bq_unavailable"}), 503
    except GoogleAPICallError as e:
        logging.error("BigQuery API error. %s", _bq_ctx(e), exc_info=True)
        return jsonify({"error": "bq_api_error"}), 502
    except RuntimeError as e:
        logging.error("BigQuery runtime error. err=%r", e, exc_info=True)
        return jsonify({"error": "bq_not_initialized"}), 503

    if row is None:
        logging.info(
            "no_candidates: query returned 0 rows. hu_id=%s mode=%s qty=%s",
            hu_id,
            mode,
            qty,
        )
        return jsonify({"error": "no_candidates"}), 409

    candidates = row.get("aisle_candidates") or []

    if not candidates:
        logging.info(
            "no_candidates: BigQuery returned empty list (likely all valid spots excluded or full)"
        )
        return jsonify({"error": "no_candidates_available"}), 409

    if not LOCKS_ENABLED:
        payload = build_payload_from_candidate(row, candidates[0])
        return jsonify(payload)

    use_aisle_locks = bool(row.get("use_aisle_locks"))

    if use_aisle_locks:
        lock_duration = 5 if mode == "REACH" else 10

        logging.info(f"Attempting to lock {len(candidates)} candidates...")

        try:
            for c in candidates:
                payload = build_payload_from_candidate(row, c)

                result = locks.reserve_aisle(
                    wh=wh_id,
                    owner_id=owner_id,
                    hu_id=hu_id,
                    zone=c["zone"],
                    aisle=c["aisle"],
                    payload=payload,
                    ttl_min=lock_duration,
                )

                if result.get("ok"):
                    if mode in ("REACH", "OP"):
                        allocs = payload[0].get("allocations") or []
                        loc_ids = [
                            a.get("location_id") for a in allocs if a.get("location_id")
                        ]

                        for loc in loc_ids:
                            locks.reserve_location(
                                wh=wh_id,
                                owner_id=owner_id,
                                hu_id=hu_id,
                                location_id=str(loc),
                                status="COOLDOWN",
                                ttl_min=11,
                            )
                            logging.info(
                                f"Locked bin {loc} for 11 mins (Data Latency Buffer) Mode: {mode}"
                            )

                    return jsonify(result["payload"])
                else:
                    logging.warning(
                        f"Failed to lock {c['zone']}:{c['aisle']} - {result.get('reason')}"
                    )

        except (RetryError, DeadlineExceeded, GoogleAPICallError):
            logging.exception("Firestore unavailable during reserve_aisle")
            return jsonify({"error": "lock_service_unavailable"}), 503
        except Exception:
            logging.exception("reserve_aisle failed")
            return jsonify({"error": "internal_error"}), 500

        return jsonify({"error": "all_candidate_aisles_locked"}), 409

    logging.error("Unexpected state: use_aisle_locks is False")
    return jsonify({"error": "configuration_error"}), 500


@app.get("/api/locks/by_aisles")
def locks_by_aisles():
    keys = request.args.get("keys", "")
    now = utcnow()

    aisle_keys = [k.strip() for k in keys.split(",") if k.strip()][:200]
    refs = [fs.collection("aisle_leases").document(f"{k}") for k in aisle_keys]

    snaps = fs.get_all(refs)
    out = []
    for snap in snaps:
        if not snap.exists:
            out.append({"aisleKey": snap.id, "locked": False})
            continue
        d = snap.to_dict() or {}
        exp = _to_utc(d.get("expiresAt"))
        locked = exp is not None and exp > now
        out.append(
            {
                "aisleKey": snap.id,
                "locked": locked,
                "ownerId": d.get("ownerId") or d.get("ownerHuId"),
                "lastHuId": d.get("lastHuId"),
                "expiresAt": d.get("expiresAt"),
            }
        )
    return jsonify(out)


@app.after_request
def no_cache_api(resp):
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/api/debug/dump_locks")
def dump_locks():
    now = utcnow()
    query = fs.collection("aisle_leases").where(
        filter=FieldFilter("expiresAt", ">", now)
    )

    active_locks = []
    print("\n========== ACTIVE AISLE LOCKS ==========")
    count = 0
    for snap in query.stream():
        d = snap.to_dict()
        exp = _to_utc(d.get("expiresAt"))
        minutes_left = round((exp - now).total_seconds() / 60, 1)

        lock_info = {
            "aisle": snap.id,
            "locked_by_owner": d.get("ownerId"),
            "locked_by_hu": d.get("ownerHuId") or d.get("lastHuId"),
            "expires_in_mins": minutes_left,
            "expires_at": str(exp),
        }

        print(
            f"🔒 {lock_info['aisle']:<10} | Owner: {lock_info['locked_by_owner']:<15} | HU: {lock_info['locked_by_hu']:<10} | Expires: {minutes_left}m"
        )
        active_locks.append(lock_info)
        count += 1

    if count == 0:
        print("✅ No active locks found.")
    print("========================================\n")

    return jsonify(active_locks)


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8080")),
        debug=debug_mode,
    )
