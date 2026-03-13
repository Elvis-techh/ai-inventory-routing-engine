# AI-Assisted Inventory Routing & Concurrency Engine (LookUp)

**Role:** Lead Developer & Operations Specialist  
**Timeline:** Peak Season 2025  
*(Note: Code and data have been sanitized/redacted to protect enterprise confidentiality).*

---

## 🚨 The Problem (The Bottleneck)
Prior to this project, the facility lacked a system-directed put-away process. Forklift drivers were forced to randomly select locations to store bulk items, creating cascading inefficiencies across the warehouse floor.

* **Poor Warehouse Slotting:** Without a direct system to suggest optimal storage bins, location utilization was highly inefficient.
* **Excessive Travel Time:** Drivers spent the majority of their shift physically traveling and visually searching for available locations, leading to dangerously low Units Per Hour (UPH).
* **Equipment Bottlenecks:** Because assignments were not dynamically routed, multiple drivers would frequently attempt to access the same narrow aisle simultaneously, causing traffic jams and decision fatigue.
* **Administrative Drain:** This compounding chaos forced the operations team to spend **15+ hours a week** manually fixing routing errors and conducting physical inventory audits.

## 💡 The Solution (The Automation)
I designed and deployed a custom, AI-assisted routing application that completely removed driver guesswork and automated the flow of heavy machinery.

* **Dynamic Routing:** The application reads live inventory data and automatically directs drivers to the most optimal, empty storage bins.
* **Single-Occupancy Protocol:** I engineered a real-time concurrency locking mechanism. When a driver is routed to a specific aisle, the system "locks" that aisle in the database, preventing any other driver from being routed there until the first driver completes their task.

## 🛠 The Tech Stack
* **Languages:** Python, SQL (GBQ)
* **Infrastructure:** Google Cloud Platform (GCP) - Cloud Run
* **Database:** Google Firestore (NoSQL)
* **Logic:** AI-assisted prompt engineering for algorithm optimization

## 📈 The Business Impact (The ROI)
* **Eliminated Collisions:** The Single-Occupancy Protocol reduced aisle traffic jams to zero, drastically improving floor safety and equipment flow.
* **Labor Hours Recovered:** Completely automated the routing process, saving the operations team 15+ hours of manual labor every single week.
* **Enterprise Adoption:** The prototype was so successful at the local level that it was formally adopted and integrated by the senior corporate IT and engineering teams.
