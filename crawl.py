import requests
import json
import re
from tqdm import tqdm
import gzip

API_KEY = "ZuK00g5OQwrnp8WTkgktU3rkw62gi5qKb0AkBmz8A16xGhov1WqDbbvOaIx10Sa3kBUqdS9hAEJJ8IS3sTpbgA=="
BASE_URL = "https://api.odcloud.kr/api/gov24/v3"

# 소관기관명 키워드 → 시·도 매핑
REGION_KEYWORDS = {
    "서울": "서울",
    "부산": "부산",
    "대구": "대구",
    "인천": "인천",
    "광주": "광주",
    "대전": "대전",
    "울산": "울산",
    "세종": "세종",
    "경기": "경기",
    "강원": "강원",
    "충북": "충북",
    "충청북도": "충북",
    "충남": "충남",
    "충청남도": "충남",
    "전북": "전북",
    "전라북도": "전북",
    "전남": "전남",
    "전라남도": "전남",
    "경북": "경북",
    "경상북도": "경북",
    "경남": "경남",
    "경상남도": "경남",
    "제주": "제주"
}

def fetch_all_data(endpoint):
    all_data = []
    page = 1
    per_page = 1000
    while True:
        url = f"{BASE_URL}/{endpoint}?page={page}&perPage={per_page}&serviceKey={API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ {endpoint} 오류: {response.status_code}")
            break
        data = response.json()
        if "data" not in data or not data["data"]:
            break
        all_data.extend(data["data"])
        if len(data["data"]) < per_page:
            break
        page += 1
    return all_data

def extract_region(agency_name):
    if not agency_name:
        return "전국", "전국"

    region_sido = "전국"
    for keyword, sido in REGION_KEYWORDS.items():
        if keyword in agency_name:
            region_sido = sido
            break

    # 시군구 이름 추출 (시·군·구 로 끝나는 단어)
    sigungu_match = re.search(r"([\w]+(시|군|구))", agency_name)
    region_sigungu = sigungu_match.group(1) if sigungu_match else "전국"

    return region_sido, region_sigungu

def merge_and_save():
    print("✅ 서비스 목록 불러오는 중...")
    service_list = fetch_all_data("serviceList")
    print(f"👉 총 {len(service_list)}건 불러옴")

    print("✅ 상세 내용 불러오는 중...")
    detail_list = fetch_all_data("serviceDetail")

    print("✅ 조건 정보 불러오는 중...")
    condition_list = fetch_all_data("supportConditions")

    print("✅ 병합 중...")
    detail_map = {d["서비스ID"]: d for d in detail_list if "서비스ID" in d}
    condition_map = {c["서비스ID"]: c for c in condition_list if "서비스ID" in c}

    merged = []
    for s in tqdm(service_list):
        sid = s.get("서비스ID", "")
        detail = detail_map.get(sid, {})
        condition = condition_map.get(sid, {})

        region_sido, region_sigungu = extract_region(s.get("소관기관명", ""))

        record = {
            "id": sid,
            "name": s.get("서비스명", ""),
            "purpose": s.get("서비스목적요약", ""),
            "target": s.get("지원대상", ""),
            "criteria": s.get("선정기준", ""),
            "details": s.get("지원내용", ""),
            "applyMethod": s.get("신청방법", ""),
            "deadline": s.get("신청기한", ""),
            "viewCount": s.get("조회수", 0),
            "agency": s.get("소관기관명", ""),
            "department": s.get("부서명", ""),
            "userType": s.get("사용자구분", ""),
            "serviceField": s.get("서비스분야", ""),
            "applyUrl": s.get("상세조회URL", ""),
            "phone": s.get("전화문의", ""),
            "registerDate": s.get("등록일시", ""),
            "updateDate": s.get("수정일시", ""),
            "fullPurpose": detail.get("서비스목적", ""),
            "requiredDocs": detail.get("구비서류", ""),
            "receptionOrg": detail.get("접수기관명", ""),
            "onlineUrl": detail.get("온라인신청사이트URL", ""),
            "law": detail.get("법령", ""),
            "condition": condition,
            "regionSido": region_sido,
            "regionSigungu": region_sigungu
        }
        merged.append(record)

    output_path = "C:/Users/admin/Documents/GitHub/benefitmatch/benefits.json.gz"
    with gzip.open(output_path, "wt", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)

    print(f"🎉 총 {len(merged)}건 저장 완료 → {output_path} (gzip 압축)")

if __name__ == "__main__":
    merge_and_save()
