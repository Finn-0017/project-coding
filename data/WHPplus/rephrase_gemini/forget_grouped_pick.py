import json
import os

# --- USER CONFIGURATION ---
# 1. Select the Name ID set you want to process ("1" to "5")
TARGET_NAME_ID = "1" 

# 2. Set the number of groups to pick (e.g., first 10 groups)
GROUPS_TO_PICK = 20
# --------------------------

INPUT_FILE = 'forget_grouped.json'

# Fixed Mapping as per requirements
NAME_IDS_MAPPING = {
    "1": ["10000", "10001"],
    "2": ["10002", "10003"],
    "3": ["10004", "10005"],
    "4": ["10006", "10007"],
    "5": ["10008", "10009"]
}

def extract_subset():
    # Construct output filename dynamically
    output_filename = f"forget_grouped_{TARGET_NAME_ID}_{GROUPS_TO_PICK}.json"
    
    # Validation
    if TARGET_NAME_ID not in NAME_IDS_MAPPING:
        print(f"Error: Name ID '{TARGET_NAME_ID}' not found in mapping.")
        return

    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please run the previous script first.")
        return

    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        full_data = json.load(f)

    subset_data = {}
    target_person_ids = NAME_IDS_MAPPING[TARGET_NAME_ID]
    
    print("-" * 60)
    print(f"Processing Set: {TARGET_NAME_ID} (IDs: {target_person_ids})")
    print(f"Keeping first {GROUPS_TO_PICK} groups per person.")
    print("-" * 60)

    found_any = False

    for person_id in target_person_ids:
        if person_id in full_data:
            found_any = True
            
            # Get all groups for this person
            all_groups = full_data[person_id]
            
            # Slice the list to get the first N groups
            # Python slicing handles cases safely even if len < GROUPS_TO_PICK
            selected_groups = all_groups[:GROUPS_TO_PICK]
            
            subset_data[person_id] = selected_groups
            
            print(f"ID {person_id}: Found {len(all_groups)} groups -> Kept {len(selected_groups)}")
        else:
            print(f"ID {person_id}: Not found in input file.")

    if not found_any:
        print("Warning: No matching IDs found in the input file. Output will be empty.")

    # Write to file
    print("-" * 60)
    print(f"Saving to {output_filename}...")
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(subset_data, f, indent=2, ensure_ascii=False)
    
    print("Done.")

if __name__ == "__main__":
    extract_subset()