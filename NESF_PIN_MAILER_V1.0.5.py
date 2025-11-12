"""
Created on Wed Oct 01 14:00:14 2025

@author: Aditya.Kumar

NESF_PIN_MAILER_V1.0.1.py: 01/10/25: [New NEFS PIN MAILER tool]
NESF_PIN_MAILER_V1.0.2.py: 06/10/25: [Remove extra pipe "|" from the end and add single space btw concating address]
NESF_PIN_MAILER_V1.0.3.py: 08/10/25: [Fix word-splitting in address fields - avoid cutting words across columns]
NESF_PIN_MAILER_V1.0.4.py: 11/10/25: [Generate separate output files for each BIN number with independent serial numbering and sorted by KitNumber]
NESF_PIN_MAILER_V1.0.5.py: 14/10/25: [Sort Dispatch Excel by BIN to match generated output order and retain headers, sanitize customer name and addresses]
"""

import glob
import pandas as pd
from datetime import date
import re
import csv
import os
import shutil
from collections import defaultdict

# === Date formats ===
today_date_ddmmyyyy = date.today().strftime('%d-%m-%Y')
today_date_for_filename = date.today().strftime('%d%m%Y')

# === Folder setup ===
input_folder = f"TODAY_DATA_{today_date_ddmmyyyy}"
output_folder = f"PROCESSED_DATA_{today_date_ddmmyyyy}"
os.makedirs(input_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

# === File movement ===
dispatch_files = glob.glob("Dispatch*.xlsx")
pin_files = glob.glob("*.pin")

if not dispatch_files:
    raise FileNotFoundError("No dispatch files found matching 'Dispatch*.xlsx'")

for file in dispatch_files + pin_files:
    shutil.move(file, os.path.join(input_folder, os.path.basename(file)))

dispatch_excel = glob.glob(os.path.join(input_folder, "Dispatch*.xlsx"))[0]
dispatch_csv = os.path.join(input_folder, "dispatch_converted.csv")

# === Address cascading with intelligent word split ===
def cascade_fields(addr1, addr2, city, state):
    def safe_split(value, max_len=25):
        value = str(value).strip()
        if len(value) <= max_len:
            return value, ""
        split_pos = value.rfind(" ", 0, max_len)
        if split_pos == -1:
            split_pos = max_len
        first = value[:split_pos].strip()
        remainder = value[split_pos:].strip()
        return first, remainder

    def safe_concat(part1, part2):
        part1, part2 = str(part1).strip(), str(part2).strip()
        if part1 and part2:
            return part1 + " " + part2
        return part1 or part2

    addr1, overflow1 = safe_split(addr1)
    addr2 = safe_concat(overflow1, addr2)

    addr2, overflow2 = safe_split(addr2)
    city = safe_concat(overflow2, city)

    city, overflow3 = safe_split(city)
    district = overflow3

    district, overflow4 = safe_split(district)
    state = safe_concat(overflow4, state)

    state, _ = safe_split(state)

    return addr1, addr2, city, district, state

# === Text cleaning ===
def clean_text(text):
    text = text.replace(",", " ")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# === Text sanitization rules ===
def clean_cust_name(name):
    """Allow only alphanumeric characters and spaces."""
    name = re.sub(r'[^A-Za-z0-9 ]', '', str(name))
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def clean_address(value):
    """
    Clean text using clean_text first, then allow only letters, numbers, spaces, and - . , / characters.
    """
    value = clean_text(value)  # apply generic cleaning
    value = re.sub(r'[^A-Za-z0-9\s\-.,/]', '', value)  # allow letters, numbers, space, - . , / :
    return value.strip()

# === Convert dispatch Excel to cleaned CSV ===
df_dispatch = pd.read_excel(dispatch_excel, engine="openpyxl", header=None, dtype=str)
df_dispatch = df_dispatch.fillna("")

with open(dispatch_csv, "w", encoding="utf-8", newline="") as fout:
    writer = csv.writer(fout, quoting=csv.QUOTE_NONE, escapechar="\\")
    for _, row in df_dispatch.iterrows():
        row = row.astype(str).tolist()
        if len(row) < 9:
            continue
        kit_no = row[8].strip()
        addr1, addr2, city, state = row[1].strip(), row[2].strip(), row[3].strip(), row[4].strip()
        pincode, mobile = row[5].strip(), row[6].strip()

        addr1, addr2, city, district, state = cascade_fields(addr1, addr2, city, state)

        addr1 = clean_address(addr1)
        addr2 = clean_address(addr2)
        city = clean_address(city)
        district = clean_address(district)
        state = clean_address(state)

        writer.writerow([kit_no, addr1, addr2, city, district, state, pincode, mobile])

# === Load dispatch data into dictionary ===
dispatch_data = {}
with open(dispatch_csv, encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if not row or len(row) < 8:
            continue
        kit_no = row[0].strip()
        if not kit_no:
            continue
        dispatch_data[kit_no] = {
            "address1": row[1],
            "address2": row[2],
            "city": row[3],
            "district": row[4],
            "state": row[5],
            "pincode": row[6],
            "mobile": row[7],
        }

# === Mask card number ===
def mask_card_number(card):
    card = str(card)
    if len(card) <= 10:
        return 'X' * max(0, len(card) - 4) + card[-4:]
    middle_len = len(card) - 10
    return card[:6] + ('X' * middle_len) + card[-4:]

# === Merge .pin and dispatch data; sort by kit_number ===
input_files = glob.glob(os.path.join(input_folder, "*.pin"))

if not input_files:
    print("No .pin files found in input folder.")
else:
    records = []

    for file in input_files:
        with open(file, encoding="cp1252") as infile:
            contents = infile.readlines()

        for line_number, line in enumerate(contents, start=1):
            if line_number == 1:
                continue

            fields = line.strip().split("|")
            while len(fields) < 19:
                fields.append("")

            try:
                account_number_full = fields[0].strip()
                account_number = account_number_full[3:15]
                raw_card = account_number_full[:16]
                masked_card = mask_card_number(raw_card)

                encrypted_block = fields[1].strip()
                bin_no = fields[2].strip()
                cust_name1 = clean_cust_name(fields[4].strip())[:25]
                kit_number = fields[13].strip()

                address1 = address2 = city = district = state = pincode = mobile = ""
                if kit_number in dispatch_data:
                    d = dispatch_data[kit_number]
                    address1 = clean_address(d["address1"])
                    address2 = clean_address(d["address2"])
                    city = clean_address(d["city"])
                    district = clean_address(d["district"])
                    state = clean_address(d["state"])
                    pincode = re.sub(r'\D', '', d["pincode"])  # digits only
                    mobile = re.sub(r'\D', '', d["mobile"])    # digits only

                records.append({
                    "bin_no": bin_no,
                    "kit_number": kit_number,
                    "masked_card": masked_card,
                    "account_number": account_number,
                    "encrypted_block": encrypted_block,
                    "cust_name": cust_name1,
                    "address1": address1,
                    "address2": address2,
                    "city": city,
                    "district": district,
                    "state": state,
                    "pincode": pincode,
                    "mobile": mobile
                })

            except Exception as e:
                print(f"Skipping line {line_number} in {file}: {e}")

    # === Group by BIN and sort each group by kit_number ===
    bin_groups = defaultdict(list)
    for r in records:
        bin_groups[r["bin_no"]].append(r)

    def safe_kit_sort(val):
        try:
            return int(val["kit_number"])
        except ValueError:
            return val["kit_number"]

    for bin_no, recs in bin_groups.items():
        recs.sort(key=safe_kit_sort)

    # === Load original dispatch with headers ===
    df_original_dispatch = pd.read_excel(dispatch_excel, engine="openpyxl", dtype=str)
    df_original_dispatch = df_original_dispatch.fillna("")

    # === Generate outputs ===
    for idx, (bin_no, recs) in enumerate(bin_groups.items(), start=1):
        total_count = len(recs)

        # CSV Output
        csv_filename = os.path.join(
            output_folder,
            f"SLCBNK-EXTPINPRINTING-{today_date_for_filename}-{idx:02d}.csv"
        )
        with open(csv_filename, "w", encoding="utf-8") as fout:
            fout.write(
                "SerialNumber|CardNumber|AccountNumber|EncryptedPinBlock|Bin|CustomerName|"
                "Address1|Address2|City|District|State|Pincode|MobileNumber|KitNumber\n"
            )
            for serial, r in enumerate(recs, start=1):
                serial_no = str(serial).zfill(6)
                fout.write(
                    f"{serial_no}|{r['masked_card']}|{r['account_number']}|{r['encrypted_block']}|"
                    f"{r['bin_no']}|{r['cust_name']}|{r['address1']}|{r['address2']}|{r['city']}|"
                    f"{r['district']}|{r['state']}|{r['pincode']}|{r['mobile']}|{r['kit_number']}\n"
                )

        # === Generate Sorted Dispatch Excel per BIN ===
        kit_order = [r["kit_number"] for r in recs]
        df_original_dispatch["__order__"] = df_original_dispatch.iloc[:, 8].apply(lambda x: str(x).strip())

        df_sorted = df_original_dispatch.set_index("__order__").reindex(kit_order).reset_index(drop=True)
        df_sorted = df_sorted.drop(columns=["__order__"], errors="ignore")

        dispatch_sorted_filename = os.path.join(
            output_folder,
            f"Dispatch_File_{today_date_ddmmyyyy}--{total_count}_BIN-{bin_no}.xlsx"
        )
        df_sorted.to_excel(dispatch_sorted_filename, index=False, header=True)

        print(f"✅ BIN {bin_no}: {total_count} records written →")
        print(f"   CSV: {csv_filename}")
        print(f"   Dispatch: {dispatch_sorted_filename}")

    print(f"\nProcessing completed. {len(bin_groups)} BIN files created in '{output_folder}'.")

input("Press ENTER to exit")
