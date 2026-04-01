flowchart LR

A[Stock Simulator] --> K
B[Social Trend Simulator] --> K

K[Kafka Streaming Layer]

K --> C[Bronze Raw Events]
C --> D[Silver Clean Events]
D --> E[Gold Aggregates]

E --> F[Anomaly Detection]

F --> G1[EWMA Detector]
F --> G2[Isolation Forest Detector]

G1 --> H[Alerts Table]
G2 --> H

H --> I[Evaluation Engine]

D --> J[Batch Recompute]
J --> K2[Batch Aggregates]

K2 --> L[Stream vs Batch Comparison]

H --> M[Streamlit Dashboard]
I --> M
L --> M

M --> N[Visualization & Monitoring]