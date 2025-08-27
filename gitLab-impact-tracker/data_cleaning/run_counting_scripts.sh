set -x

python count_lines.py Persons_and_Affiliations.csv Pure_Org_Units.csv
python count_db.py final_impact_tracker.db --org-id 1
python3 pure_id_tools.py Persons_and_Affiliations.csv --find "Michael" "Rovatsos"
python3 pure_id_tools.py Persons_and_Affiliations.csv --find "Rosa" "Filgueira"
