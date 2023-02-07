# README

OD-matrix-aggregator aggregates trip data into periods of 4 hours (2-6, 6-10, 10-14, 14-18, 18-22, 22-2) and updates the data after 1, 3, 7 and 30 days. 

This software can be run locally by running
```bash
python3 -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
python main.py
```

# ssh tunnels
ssh -L 5433:10.133.75.95:5432 root@auth.deelfietsdashboard.nl