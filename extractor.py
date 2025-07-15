#!/usr/bin/env python3
import re, argparse, yaml, pandas as pd
from pathlib import Path
import pdfplumber
from docx import Document

LABELS = """
last first dob cin asm_date a_present a_source a_mode caregiver_assist a_goc a_omcg a_cgcomm a_lvstatus a_lvarr a_ed a_sect_comments
b_shortmem b_procmem b_sect_comments c_sect_comments d_pleasure d_anxious d_sad d_sect_comments
e_social e_family e_other e_alone e_stress e_sect_comments
f_mealperf f_mealcap f_hswperf f_hswcap f_fncperf f_fnccap f_medperf f_medcap f_phnperf f_phncap f_stairperf f_staircap
f_shopperf f_shopcap f_transperf f_transcap f_bathing f_hygiene f_dressup f_dresslow f_walk f_loco f_transtoilet f_toiletuse
f_bedmob f_eating f_mode f_exercise f_out f_adlchange f_suffchange f_drove f_stopdrv f_toltrans f_sect_comments
g_bladder g_bowel g_sect_comments
h_hip h_other h_alz h_demen h_stroke h_chd h_copd h_chf h_anx h_bpd h_depr h_schiz h_covid h_cancer h_dm h_sect_comments
i_falls i_noinj i_mininj i_majinj i_dizzi i_gait i_chest i_atp i_ffb i_hallu i_reflux i_const i_diarr i_vomit i_nonsleep i_toosleep
i_dyspnea i_fat i_painfreq i_painint i_paincons i_painbrkt i_paincntrl i_cond i_exp i_health i_smoke i_chew i_drinks i_drinkcut
i_drinkcrit i_drinkguilt i_drinkeye i_drinksoc i_sect_comments
j_weight j_dehyd j_fluidin j_fluidout j_mode j_sect_comments
k_rx k_allergy k_allcat k_allother k_sect_comments
l_bp l_colon l_dental l_eye l_hearing l_influ l_mammo l_pneu l_covid l_inpatient l_er l_phys l_facility l_impmed l_inj l_resp l_wound
l_hhdiab l_gibleed l_heart l_mcis l_chemo l_surg l_uti l_iv l_dvtpe l_pain l_psycho l_other l_unknown l_impmeder l_nausea l_injer
l_resper l_wounder l_cardiac l_hhdiaber l_gibleeder l_otherer l_unknowner l_therapy l_respite l_eolc l_perm l_unsafe l_othernh
l_unknh l_sect_comments
m_family m_commun m_sect_comments
n_food n_shelter n_clothing n_meds n_hvac n_health n_sect_comments
""".split()

for i in range(1, 27):
    LABELS.extend([
        f"ma_drug{i}", f"mad{i}", f"ma_unit{i}", f"ma_route{i}", f"ma_frq{i}",
        f"p{i}", f"ma_notes{i}", f"notes{i}"
    ])

LABELS.extend("""
chad_bp chad_copd chad_dm chad_heart chad_hip chad_odem chad_ofrac
fsd_hemi fsd_ms fsd_para fsd_park fsd_pneu
od_d1 od_dd1 od_icd1 od_d2 od_dd2 od_icd2 od_d3 od_dd3 od_icd3 od_d4 od_dd4 od_icd4
sec_age sec_loc sec_120 sec_adl1 sec_adl2 sec_adl3 sf_120 sf_sched sf_alone
""".split())

def read_pdf(path: Path) -> str:
    with pdfplumber.open(path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)

def read_docx(path: Path) -> str:
    return "\n".join(p.text for p in Document(path).paragraphs)

def load_text(path: Path) -> str:
    return read_pdf(path) if path.suffix.lower() == ".pdf" else read_docx(path)

def first_n_sentences(text: str, n=2) -> str:
    parts = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text.strip())
    return " ".join(parts[:n])

def sectionize(text: str) -> dict:
    sections, cur = {}, "_preamble"
    for line in text.splitlines():
        m = re.match(r"^([\w ,/()]+):\s*$", line)
        if m and (line.isupper() or line.istitle()):
            cur = m.group(1).lower()
            sections[cur] = []
        else:
            sections.setdefault(cur, []).append(line)
    return {k: "\n".join(v).strip() for k, v in sections.items()}

def load_yaml() -> dict:
    yml = Path("label_map.yml")
    return yaml.safe_load(yml.read_text()) if yml.exists() else {}

def expand_wildcards(rules: dict, max_n=30) -> dict:
    out = {}
    for lab, rule in rules.items():
        if "*" in lab:
            for i in range(1, max_n + 1):
                out[lab.replace("*", str(i))] = {**rule, "row": i-1}
        else:
            out[lab] = rule
    return out

def extract(path: Path) -> dict:
    text = load_text(path)
    sections = sectionize(text)
    rules = expand_wildcards(load_yaml())
    row = {lab: "" for lab in LABELS}
    for label in LABELS:
        if row[label]:
            continue
        rule = rules.get(label) or {"search": [label.replace('_', ' ')], "type": "single_line"}
        rule_type = rule["type"]
        variants = rule.get("search", []) if rule_type != "regex" else []
        cand_secs = [s for n, s in sections.items() if any(re.search(v, n, re.I) for v in variants)] or [text]
        val = ""
        if rule_type == "single_line":
            for sec in cand_secs:
                for v in variants:
                    pat = rf"{re.escape(v)}[\s:]*(.+?)(?=\s{{2,}}|\n|$)"
                    m = re.search(pat, sec, flags=re.I)
                    if m:
                        val = m.group(1).strip()
                        break
                if val:
                    break
        elif rule_type == "multi_line":
            for sec in cand_secs:
                for v in variants:
                    pat = rf"{re.escape(v)}[\s:]*(.+?)(?=\n[A-Z0-9 ,/()]+:\s|\n\s*\n|$)"
                    m = re.search(pat, sec, flags=re.I | re.S)
                    if m:
                        val = " ".join(m.group(1).splitlines()).strip()
                        break
                if val:
                    break
        elif rule_type == "paragraph":
            val = first_n_sentences(cand_secs[0], rule.get("keep_n_sentences", 2))
        elif rule_type == "regex":
            pat = rule["pattern"]
            m = re.search(pat, text, flags=re.I | re.S | re.M)
            if not m:
                for sec in cand_secs:
                    m = re.search(pat, sec, flags=re.I | re.S | re.M)
                    if m:
                        break
            val = m.group(1).strip() if m else ""
        row[label] = val
    return row

def write_row(row, headers, out_path):
    df = pd.DataFrame([[row.get(h, "") for h in headers]], columns=headers)
    (df.to_excel if out_path.endswith(".xlsx") else df.to_csv)(out_path, index=False)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("-o", "--out", default="output.xlsx")
    args = ap.parse_args()
    write_row(extract(Path(args.file)), LABELS, args.out)
    print(f"Saved {args.out}")
