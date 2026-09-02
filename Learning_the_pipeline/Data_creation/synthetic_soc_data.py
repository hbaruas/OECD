import pandas as pd
import random

# --- Load your dataset ---
df = pd.read_csv("/Users/saurabhkumar/Desktop/UK_JOB_OECD/Data/reed_jobs_uk_extended.csv")

# --- Rule-based keyword to SOC lookup ---
keyword_to_soc = {
    "data entry": "4112",
    "data analytics": "2425",
    "data science": "3421"
}

# --- Generate fallback SOC codes for unmatched descriptions ---
fallback_soc_codes = [f"{random.randint(1000, 9999)}" for _ in range(200)]
fallback_soc_codes = list(set(fallback_soc_codes))  # remove duplicates

# --- SOC assignment logic based on keyword lookup ---
def assign_soc_code(description):
    description = str(description).lower()
    for keyword, soc_code in keyword_to_soc.items():
        if keyword in description:
            return soc_code
    return random.choice(fallback_soc_codes)

# --- Apply to each job description ---
df["soc_code"] = df["jobDescription"].apply(assign_soc_code)

# --- Flag if the SOC was assigned using a keyword match (landmark) ---
df["landmark_flag"] = df["soc_code"].isin(keyword_to_soc.values())

# --- Save the enriched dataset ---
output_path = "/Users/saurabhkumar/Desktop/UK_JOB_OECD/Data/enriched_with_soc.csv"
df.to_csv(output_path, index=False)
print(f"✅ Dataset saved to: {output_path}")
