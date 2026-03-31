Please follow the ./report/Assigment-2-Deployement.md to execute the whole project properly.

Folders in code:

mysimbdp/  
├── cache/ ----> stores the tenant cache locally, per tenant as separate folder
│ ├── tenantA/pending/
│ ├── tenantA/processed/
│ ├── tenantB/pending/
│ └── tenantB/processed/
├── platform/ ----> platform main code files
│ ├── batchmanager.py
│ ├── streamingestmanager.py
│ └── streamingestmonitor.py
└── tenants/ ----> tenant main code files, separate folder for each tenant
├── tenantA/
│ ├── tenantA_producer.py
│ ├── streamingestworker.py
│ ├── silverpipeline_tenantA.py
│ └── tenantA.yaml ----> SLA file
└── tenantB/
├── tenantB_producer.py
├── streamingestworker.py
├── silverpipeline_tenantB.py
└── tenantB.yaml ----> SLA file
