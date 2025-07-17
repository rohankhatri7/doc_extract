import os
import pdfplumber
from azure.ai.formrecognizer import DocumentAnalysisClient

# Re‑use the same client and model_id you configured earlier
document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint,
    credential=AzureKeyCredential(key)
)
model_id = "form_classifier_model_id"  # put your classifier model ID here

classification_cache = {}  # {(pdf_path, page_number): {doc_type, confidence, model_id}}

def classify_each_page(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                temp_path = f"__temp_page_{i}.png"
                page.to_image(resolution=300).save(temp_path, format="PNG")
                with open(temp_path, "rb") as f:
                    poller = document_analysis_client.begin_classify_document(
                        model_id, document=f
                    )
                    result = poller.result()
                    if result and result.documents:
                        classification_cache[(pdf_path, i)] = {
                            "doc_type": result.documents[0].doc_type,
                            "confidence": result.documents[0].confidence,
                            "model_id": result.model_id,
                        }
                os.remove(temp_path)
    except Exception as e:
        print(f"Error classifying pages in {pdf_path}: {e}")
