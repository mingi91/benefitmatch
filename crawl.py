import requests
import json
import re
from tqdm import tqdm
import gzip

API_KEY = "ZuK00g5OQwrnp8WTkgktU3rkw62gi5qKb0AkBmz8A16xGhov1WqDbbvOaIx10Sa3kBUqdS9hAEJJ8IS3sTpbgA=="
BASE_URL = "https://api.odcloud.kr/api/gov24/v3"

# 시군구 → 시도 매핑 테이블
SIGUNGU_TO_SIDO = {
    # 서울
    "종로": "서울", "중구": "서울", "용산": "서울", "성동": "서울", "광진": "서울", "동대문": "서울",
    "중랑": "서울", "성북": "서울", "강북": "서울", "도봉": "서울", "노원": "서울", "은평": "서울",
    "서대문": "서울", "마포": "서울", "양천": "서울", "강서": "서울", "구로": "서울", "금천": "서울",
    "영등포": "서울", "동작": "서울", "관악": "서울", "서초": "서울", "강남": "서울", "송파": "서울", "강동": "서울",
    # 부산
    "중구(부산)": "부산", "서구(부산)": "부산", "동구(부산)": "부산", "영도": "부산", "부산진": "부산",
    "동래": "부산", "남구(부산)": "부산", "북구(부산)": "부산", "해운대": "부산", "사하": "부산",
    "금정": "부산", "강서(부산)": "부산", "연제": "부산", "수영": "부산", "사상": "부산", "기장": "부산",
    # 대구
    "달성": "대구", "달서": "대구", "수성": "대구", "북구(대구)": "대구", "남구(대구)": "대구",
    "동구(대구)": "대구", "서구(대구)": "대구", "중구(대구)": "대구", "군위": "대구",
    # 광주
    "광산": "광주", "북구(광주)": "광주", "남구(광주)": "광주", "동구(광주)": "광주", "서구(광주)": "광주",
    # 전북
    "전주": "전북", "군산": "전북", "익산": "전북", "정읍": "전북", "남원": "전북", "김제": "전북",
    # 경북
    "포항": "경북", "구미": "경북", "경산": "경북", "김천": "경북", "안동": "경북", "영주": "경북",
    "영천": "경북", "상주": "경북", "문경": "경북", "경주": "경북",
    # 충남
    "부여": "충남", "공주": "충남", "천안": "충남", "논산": "충남", "서산": "충남", "당진": "충남",
    "보령": "충남", "아산": "충남", "계룡": "충남",
    # 경기 (대표 예시)
    "수원": "경기", "성남": "경기", "안양": "경기", "부천": "경기", "고양": "경기",
    "용인": "경기", "남양주": "경기", "화성": "경기", "평택": "경기", "시흥": "경기",
    "파주": "경기", "의정부": "경기", "광명": "경기", "군포": "경기", "안산": "경기",
    "구리": "경기", "김포": "경기", "하남": "경기", "이천": "경기", "오산": "경기", "양주": "경기",
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

    # 1) 시군구 보정 테이블에서 키워드 매칭
    for key, sido in SIGUNGU_TO_SIDO.items():
        if key in agency_name:
            return sido, key

    # 2) 시도명 직접 매칭
    sido_match = re.search(r"(서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주)", agency_name)
    region_sido = sido_match.group(1) if sido_match else "전국"

    return region_sido, "전국"

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
