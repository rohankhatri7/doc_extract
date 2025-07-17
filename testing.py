import json
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.exceptions import HttpResponseError
import glob
import os
import time
import pandas as pd
import re
import format_documents
from dotenv import load_dotenv
from ratelimit import limits, RateLimitException
import pdfplumber
import io
import tempfile

load_dotenv()
endpoint = os.getenv("AZURE_DOC_INTELLIGENCE_ENDPOINT")
key = os.getenv("AZURE_DOC_INTELLIGENCE_KEY")

CALLS = 1
RATE_LIMIT_PERIOD = 60

def model_call(document_path, model_id):
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key)
    )
    with open(document_path, "rb") as f:
        poller = document_analysis_client.begin_analyze_document(
            model_id=model_id,
            document=f
        )
    result = poller.result()
    return result

def model_call_bytes(document_bytes, model_id):
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key)
    )
    poller = document_analysis_client.begin_analyze_document(
        model_id=model_id,
        document=document_bytes
    )
    result = poller.result()
    return result

def id_model_result(directory_path, excel_path):
    results = []
    search_pattern = os.path.join(directory_path, '*')
    jpg_files = glob.glob(search_pattern)
    for jpg_file in jpg_files:
        try:
            result = model_call(jpg_file, model_id="prebuilt-idDocument")
        except RateLimitException as e:
            time.sleep(e.period_remaining)
            print(f"Rate limit reached. Sleeping for {e.period_remaining} seconds...")
        print(f"Processed {jpg_file} with ID model.")
        output = format_documents.format_id_document(result.documents[0], "idDocument")
        first_name = output.get("first_name", "")
        last_name = output.get("last_name", "")
        ssn = output.get("ssn", "")
        dob = output.get("dob", "")
        address_parts = [
            output.get("street1", ""),
            output.get("city", ""),
            output.get("state", ""),
            output.get("zip_code", "")
        ]
        address = ", ".join([part for part in address_parts if part])
        results.append({
            "file_name": os.path.basename(jpg_file),
            "first_name": first_name,
            "last_name": last_name,
            "ssn": ssn,
            "dob": dob,
            "address": address,
        })
    if results:
        df = pd.DataFrame(results)
        upsert_to_excel(df, "id_model", excel_path)

def tax_return_model_result(directory_path, excel_path):
    results = []
    search_pattern = os.path.join(directory_path, '*')
    jpg_files = glob.glob(search_pattern)
    for jpg_file in jpg_files:
        with open(jpg_file, "rb") as f:
            result = model_call(jpg_file, model_id="prebuilt-tax.us.w2")
        print(f"Processed {jpg_file} with tax return model.")
        if result:
            out = format_documents.format_tax_document(result.documents[0], "prebuilt-tax.us.w2")
            first_name = out.get("first_name", "")
            last_name = out.get("last_name", "")
            ssn = out.get("ssn", "")
            address_parts = [
                out.get("street1", ""),
                out.get("city", ""),
                out.get("state", ""),
                out.get("zip_code", "")
            ]
            address = ", ".join([part for part in address_parts if part])
            results.append({
                "file_name": os.path.basename(jpg_file),
                "first_name": first_name,
                "last_name": last_name,
                "address": address,
                "ssn": ssn,
                "dob": ""
            })
    if results:
        df = pd.DataFrame(results)
        upsert_to_excel(df, "Tax_Returns", excel_path)

def default_model_result(directory_path, excel_path):
    results = []
    search_pattern = os.path.join(directory_path, '*')
    jpg_files = glob.glob(search_pattern)
    first_name_pattern = re.compile(r"(first\s*name|first\s*initial|employee.*first.*name|emp.*first.*name)", re.I)
    last_name_pattern = re.compile(r"(last\s*name|employee.*last.*name|emp.*last.*name)", re.I)
    address_pattern = re.compile(r"(address|zip\s*code|employee.*address|emp.*address)", re.I)
    ssn_pattern = re.compile(r"(ssn|social\s*security|social.*number|employee.*ssn|emp.*ssn)", re.I)
    paystub_name_pattern = re.compile(r"pay to the order of[:\-#]*", re.I)
    paystub_ssn_pattern = re.compile(r"social security[\s#:.\-]*", re.I)
    for jpg_file in jpg_files:
        with open(jpg_file, "rb") as f:
            try:
                result = model_call(jpg_file, model_id="prebuilt-document")
            except RateLimitException as e:
                time.sleep(e.period_remaining)
                print(f"Rate limit reached. Sleeping for {e.period_remaining} seconds...")
        print(f"Processed {jpg_file} with default model.")
        if "Social_Security" in jpg_file:
            output = format_documents.extract_ssn_fields(result)
            results.append({
                "file_name": os.path.basename(jpg_file),
                "first_name": output.get("first_name", ""),
                "last_name": output.get("last_name", ""),
                "address": "",
                "ssn": output.get("ssn", ""),
                "dob": ""
            })
        elif "Employee_Auth" in jpg_file:
            sheet2 = pd.read_excel("test_docs_results.xlsx", sheet_name='configs')
            config_row = sheet2[sheet2['form_type'] == "employee_auth"]
            if not config_row.empty:
                key_mapping_str = config_row['key_mapping'].iloc[0]
                key_mapping = json.loads(key_mapping_str)
                output = format_documents.format_generic_document(result, key_mapping, "employee_auth")
                results.append({
                    "file_name": os.path.basename(jpg_file),
                    "first_name": output.get("first_name", ""),
                    "last_name": output.get("last_name", ""),
                    "address": "",
                    "ssn": "",
                    "dob": output.get("dob", "")
                })
        elif "Paystub" in jpg_file or "Paycheck_Stubs" in jpg_file:
            full_name = ""
            ssn = ""
            for kv_pair in result.key_value_pairs:
                key_content = kv_pair.key.content if kv_pair.key else ""
                value_content = kv_pair.value.content if kv_pair.value else ""
                if paystub_name_pattern.match(key_content.strip()) and not full_name:
                    full_name = value_content
                elif paystub_ssn_pattern.match(key_content.strip()) and not ssn:
                    ssn = value_content
            name_parts = full_name.split()
            first_name, middle_initial, last_name = "", "", ""
            if len(name_parts) == 2:
                first_name, last_name = name_parts
            elif len(name_parts) == 3:
                first_name, middle_initial, last_name = name_parts
            elif len(name_parts) > 3:
                first_name = name_parts[0]
                middle_initial = name_parts[1]
                last_name = " ".join(name_parts[2:])
            elif len(name_parts) == 1:
                first_name = name_parts[0]
            results.append({
                "file_name": os.path.basename(jpg_file),
                "first_name": first_name,
                "last_name": last_name,
                "address": "",
                "ssn": ssn,
                "dob": ""
            })
        else:
            first_name = last_name = address = ssn = ""
            for kv_pair in result.key_value_pairs:
                key_content = kv_pair.key.content if kv_pair.key else ""
                value_content = kv_pair.value.content if kv_pair.value else ""
                if first_name_pattern.search(key_content) and not first_name:
                    name_parts = value_content.split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = name_parts[-1]
                    else:
                        first_name = value_content
                elif last_name_pattern.search(key_content) and not last_name:
                    last_name = value_content
                elif address_pattern.search(key_content) and not address:
                    address = value_content
                elif ssn_pattern.search(key_content) and not ssn:
                    ssn = value_content
            results.append({
                "file_name": os.path.basename(jpg_file),
                "first_name": first_name,
                "last_name": last_name,
                "address": address,
                "ssn": ssn,
                "dob": ""
            })
    if results:
        df = pd.DataFrame(results)
        upsert_to_excel(df, "Generic_Forms", excel_path)

def upsert_to_excel(df_new, sheet_name, excel_path):
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            try:
                df_existing = pd.read_excel(excel_path, sheet_name=sheet_name)
            except Exception:
                df_existing = pd.DataFrame(columns=df_new.columns)
            df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["file_name"], keep='last')
            df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
    except FileNotFoundError:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df_new.to_excel(writer, sheet_name=sheet_name, index=False)

def single_doc_testing(doc_path, model_id):
    placeholder_set = set()
    with pdfplumber.open(doc_path) as pdf:
        for page in pdf.pages:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                page.to_image(resolution=300).save(tmp.name, format="PNG")
                with open(tmp.name, "rb") as f:
                    result = model_call_bytes(f, model_id=model_id)
                os.remove(tmp.name)
            for kv_pair in result.key_value_pairs:
                if kv_pair.key and kv_pair.value:
                    print(f"Key: '{kv_pair.key.content}' -> Value: '{kv_pair.value.content}'")
                elif kv_pair.key:
                    print(f"Key: '{kv_pair.key.content}' -> Value: ''")
            for kv_pair in result.key_value_pairs:
                if kv_pair.value:
                    val = kv_pair.value.content.strip()
                    m = re.fullmatch(r"\{\{\s*([^}]+?)\s*\}\}", val)
                    if m:
                        placeholder_set.add(m.group(1))
    label_list_str = """<LABEL_LIST_PLACEHOLDER>"""
    label_set = {lbl.strip() for lbl in label_list_str.split(",")}
    total_labels = len(label_set)
    matches = placeholder_set & label_set
    print(f"\nMatched {len(matches)}/{total_labels} placeholders")

if __name__ == "__main__":
    single_doc_testing("template-nogridlines-5.21.1.pdf", "prebuilt-document")
