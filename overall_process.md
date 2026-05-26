# Συνολική Διαδικασία (Markdown Overall Process)

## 1. Ingestion & Preprocessing Layer

**Upload:** Το raw video της κυκλοφορίας (διάρκειας 32 λεπτών, 25fps) αναρτάται στο Azure Blob Storage (Input Container).

**Trigger:** Η ολοκλήρωση του upload ενεργοποιεί αυτόματα μια Azure Function (Blob Trigger).

**Orchestration:** Η Function δεν κάνει επεξεργασία η ίδια· καλεί το API του Azure Container Apps για να εκκινήσει ένα Preprocessing Container Job.

**Segmentation:** Το Preprocessing Job κατεβάζει το βίντεο, χρησιμοποιεί FFmpeg και το τεμαχίζει σε ανεξάρτητα, μικρότερα κλιπ των 2 λεπτών.

**State & Notification:** Τα 2-min clips αποθηκεύονται σε ξεχωριστό container στο Blob Storage. Για κάθε έτοιμο κλιπ, το Job δημοσιεύει (Publish) ένα event στο κεντρικό Azure Event Hubs (Kafka-compatible) με το URI του αρχείου.

## 2. Stream Processing & Computer Vision Layer

**Horizontal Scaling:** Το Azure Container Apps (CV Workers Cluster) παρακολουθεί το Event Hub και κλιμακώνει οριζόντια τα instances (έως 16 ταυτόχρονα, ένα για κάθε clip).

**Core Inference:** Κάθε Worker καταναλώνει το αντίστοιχο μήνυμα, κατεβάζει το clip και εκτελεί:

- **YOLOv8:** Για ανίχνευση (detection), ταξινόμηση (ΙΧ vs Φορτηγά) και απόδοση μοναδικού Tracking ID σε κάθε όχημα.
- **OpenCV:** Για τον υπολογισμό της ταχύτητας.
- **Perspective Transformation (Bonus +10%):** Αντί για την απλοϊκή γραμμική προσέγγιση των 17 μέτρων, ο κώδικας εφαρμόζει μετασχηματισμό προοπτικής (homography matrix) με βάση το πραγματικό μήκος των 25m των πράσινων γραμμών για εξάλειψη του σφάλματος βάθους.

## 3. Split Flow: Real-Time Alerts vs. Analytics Storage

Μετά την επεξεργασία των frames, η ροή διακλαδίζεται αυστηρά σε δύο υπο-ροές:

### Α. Ροή Πραγματικού Χρόνου (Real-Time Alerts Line)

**Condition:** Εάν η υπολογισμένη ταχύτητα ενός οχήματος (βάσει του Tracking ID του) ξεπεράσει τα $130\text{ km/h}$.

**Ingestion:** Ο Worker παράγει ένα event με το schema του οχήματος και το κάνει publish σε ένα ξεχωριστό Topic στο Azure Event Hubs (π.χ. speed-violations).

**Execution:** Μια αποκλειστική Azure Function (Event Hub Trigger) καταναλώνει άμεσα το event και τυπώνει τα στοιχεία του συγκεκριμένου οχήματος (ID, ταχύτητα, λωρίδα) στο log stream της, εξασφαλίζοντας real-time απομονωμένο reporting ανά παράβαση.

### Β. Ροή Ανάλυσης & Αναφορών (Batch/Reporting Line)

**Raw Output:** Οι Workers γράφουν τα analytical αποτελέσματα (ID, vehicle type, speed, timestamp, lane, direction) σε αρχεία JSON στο Blob Storage (Analytics Storage).

**Consolidation (The Cron Approach):** Μια Azure Function (Timer Trigger) εκτελείται προγραμματισμένα (π.χ. μία φορά στο τέλος του computation).

**Aggregation:** Η συνάρτηση διαβάζει το σύνολο των JSON αρχείων και για τα 32 λεπτά του βίντεο, εκτελεί stateful aggregation και παράγει το τελικό Log File / Report, το οποίο περιλαμβάνει:

- Συνολικό αριθμό οχημάτων ανά ρεύμα.
- Συνολικό αριθμό παραβατών (>$90\text{ km/h}$ για ΙΧ, >$80\text{ km/h}$ για φορτηγά).
- Αριθμό οχημάτων ανά ρεύμα και ανά 5λεπτο.
- Μέση ταχύτητα ανά ρεύμα και ανά 5λεπτο (π.χ. inbound, 1st 5min, 60kmh).

## 4. Business Metrics Layer (Prometheus / Grafana)

**Architecture:** Τα Prometheus & Grafana εκτελούνται ως Container Apps στο ήδη υπάρχον Azure Container Apps Environment (`vana-traffic-env`), αξιοποιώντας την ίδια υποδομή με τους CV Workers.

**Metrics Collection:** Καθώς οι CV Workers είναι εφήμεροι (ephemeral), κάνουν push business metrics στον Prometheus Pushgateway (επίσης Container App) αμέσως μετά την επεξεργασία κάθε clip. Τα metrics που παράγονται αφορούν αποκλειστικά τα ερωτήματα της εργασίας:

- **Q2:** Ποσοστό φορτηγών ανά λωρίδα (`truck_ratio_per_lane`)
- **Q3:** Αριθμός παραβατών ανά τύπο οχήματος (`speeders_total`)
- **Q5:** Αριθμός οχημάτων ανά λωρίδα και ανά 5λεπτο (`vehicle_count_per_lane`)
- **Q7:** Μέση ταχύτητα ανά λωρίδα και ανά 5λεπτο (`avg_speed_per_lane`)
- **Q8:** Φορτηγά εκτός τέρμα αριστερής λωρίδας (`trucks_not_far_left_total`)
- **Latency:** Χρόνος επεξεργασίας κάθε clip σε histogram (`clip_processing_duration_seconds`)

**Visualization:** Ο Prometheus κάνει scrape τον Pushgateway και το Grafana απεικονίζει τα παραπάνω business metrics σε πραγματικό χρόνο κατά τη διάρκεια της επεξεργασίας.

```mermaid
flowchart TD
    classDef storage fill:#1f77b4,stroke:#fff,stroke-width:2px,color:#fff
    classDef compute fill:#2ca02c,stroke:#fff,stroke-width:2px,color:#fff
    classDef messaging fill:#ff7f0e,stroke:#fff,stroke-width:2px,color:#fff
    classDef monitor fill:#9467bd,stroke:#fff,stroke-width:2px,color:#fff
    classDef external fill:#7f7f7f,stroke:#fff,stroke-width:2px,color:#fff

    RAW([Raw Video: 32-min .mp4]) -->|1. Upload| ST1[(Blob Storage: Input Container)]
    ST1 -->|2. Event Trigger| FN1[Azure Function: Blob Trigger]
    FN1 -->|3. Starts Job| APP1

    APP1 -->|4. Split and Save Clips| ST2[(Blob Storage: 2-min Clips)]
    APP1 -->|5. Publish Event per Clip| EH1([Event Hubs: Clips Topic])

    EH1 -->|6. Consume Event| APP2
    ST2 -.->|7. Download Clip| APP2

    APP2 -->|8. Run Inference| YOLO

    PT -->|8a. Speed over 140 km/h| EH2([Event Hubs: Violations Topic])
    EH2 -->|9a. Trigger| FN2[Azure Function: Alert Logger]
    FN2 -->|10a. Write| LOG1[[Real-time Alerts Log]]
    FN2 -->|Q3: Push violation metrics| GW

    PT -->|8b. Write Raw Inferences| ST3[(Blob Storage: Raw JSON Results)]
    ST3 -.->|9b. Timer Read| FN3[Azure Function: Batch Report Generator]
    FN3 -->|10b. Generate Aggregates| LOG2[[final report 32min.log]]
    FN3 -->|Q2 Q5 Q7 Q8: Push aggregated metrics| GW

    APP2 -->|8c. Push CV latency histogram| GW

    APP1[Container App Job: Preprocessing]
    APP2[Container Apps: CV Workers Cluster]

    subgraph CV_Worker [CV Worker Engine]
        YOLO[YOLOv8: Detection and Tracking] -->|Frames| OCV[OpenCV: Speed Estimation]
        OCV -->|Homography| PT[Perspective Transformation - Bonus 10pct]
    end

    GW[Prometheus Pushgateway: Container App]
    PROM[Prometheus Server: Container App] -->|Visualize| GRAF[Grafana Dashboard: Container App]
    GW -.->|Scrape| PROM

    class ST1,ST2,ST3 storage
    class FN1,FN2,FN3,APP1,APP2 compute
    class EH1,EH2 messaging
    class PROM,GRAF,GW monitor
    class RAW,LOG1,LOG2 external
```
