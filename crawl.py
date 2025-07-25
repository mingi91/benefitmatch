import requests
import json
import re
from tqdm import tqdm
import gzip

API_KEY = "ZuK00g5OQwrnp8WTkgktU3rkw62gi5qKb0AkBmz8A16xGhov1WqDbbvOaIx10Sa3kBUqdS9hAEJJ8IS3sTpbgA=="
BASE_URL = "https://api.odcloud.kr/api/gov24/v3"

# ---------------- fetch_all_data ----------------
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

# ---------------- REGION_KEYWORDS ----------------
REGION_KEYWORDS = {
    # 광역시·도
    "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천",
    "광주": "광주", "대전": "대전", "울산": "울산", "세종": "세종",
    "경기": "경기", "강원": "강원", "충북": "충북", "충청북도": "충북",
    "충남": "충남", "충청남도": "충남", "전북": "전북", "전라북도": "전북",
    "전남": "전남", "전라남도": "전남", "경북": "경북", "경상북도": "경북",
    "경남": "경남", "경상남도": "경남", "제주": "제주",

    # 경기도
    "수원시": "경기","성남시": "경기","고양시": "경기","용인시": "경기","부천시": "경기","안산시": "경기",
    "안양시": "경기","남양주시": "경기","화성시": "경기","평택시": "경기","의정부시": "경기","시흥시": "경기",
    "파주시": "경기","김포시": "경기","광명시": "경기","광주시": "경기","군포시": "경기","이천시": "경기",
    "오산시": "경기","양주시": "경기","구리시": "경기","안성시": "경기","포천시": "경기","의왕시": "경기",
    "하남시": "경기","여주시": "경기","양평군": "경기","가평군": "경기","연천군": "경기",

    # 강원도
    "춘천시": "강원","원주시": "강원","강릉시": "강원","동해시": "강원","태백시": "강원","속초시": "강원",
    "삼척시": "강원","홍천군": "강원","횡성군": "강원","영월군": "강원","평창군": "강원","정선군": "강원",
    "철원군": "강원","화천군": "강원","양구군": "강원","인제군": "강원","고성군": "강원","양양군": "강원",

    # 충청북도
    "청주시": "충북","충주시": "충북","제천시": "충북","보은군": "충북","옥천군": "충북","영동군": "충북",
    "진천군": "충북","괴산군": "충북","음성군": "충북","단양군": "충북",

    # 충청남도
    "천안시": "충남","공주시": "충남","보령시": "충남","아산시": "충남","서산시": "충남","논산시": "충남",
    "계룡시": "충남","당진시": "충남","금산군": "충남","부여군": "충남","서천군": "충남","청양군": "충남",
    "홍성군": "충남","예산군": "충남","태안군": "충남",

    # 전라북도
    "전주시": "전북","전주": "전북","군산시": "전북","익산시": "전북","정읍시": "전북","남원시": "전북","김제시": "전북",
    "완주군": "전북","진안군": "전북","무주군": "전북","장수군": "전북","임실군": "전북","순창군": "전북",
    "고창군": "전북","부안군": "전북",

    # 전라남도
    "목포시": "전남","여수시": "전남","순천시": "전남","나주시": "전남","광양시": "전남","담양군": "전남",
    "곡성군": "전남","구례군": "전남","고흥군": "전남","보성군": "전남","화순군": "전남","장흥군": "전남",
    "강진군": "전남","해남군": "전남","영암군": "전남","무안군": "전남","함평군": "전남","영광군": "전남",
    "장성군": "전남","완도군": "전남","진도군": "전남","신안군": "전남",

    # 경상북도
    "포항시": "경북","경주시": "경북","김천시": "경북","안동시": "경북","구미시": "경북","영주시": "경북",
    "영천시": "경북","상주시": "경북","문경시": "경북","경산시": "경북","군위군": "경북","의성군": "경북",
    "청송군": "경북","영양군": "경북","영덕군": "경북","청도군": "경북","고령군": "경북","성주군": "경북",
    "칠곡군": "경북","예천군": "경북","봉화군": "경북","울진군": "경북","울릉군": "경북",

    # 경상남도
    "창원시": "경남","진주시": "경남","통영시": "경남","사천시": "경남","김해시": "경남","밀양시": "경남",
    "거제시": "경남","양산시": "경남","의령군": "경남","함안군": "경남","창녕군": "경남","고성군": "경남",
    "남해군": "경남","하동군": "경남","산청군": "경남","함양군": "경남","거창군": "경남","합천군": "경남",

    # 제주
    "제주시": "제주","서귀포시": "제주",

    # 광역시 하위 구·군
    "달성군": "대구", "달성": "대구","달서구": "대구", "달서": "대구","수성구": "대구", "수성": "대구",
    "해운대구": "부산", "해운대": "부산",

    # 모호 → 전국 처리 필요
    "중구": "전국","남구": "전국","동구": "전국","서구": "전국","북구": "전국"
}

AMBIGUOUS_NAMES = {"중구", "남구", "동구", "서구", "북구"}

# ---------------- extract_region ----------------
def extract_region(agency_name: str):
    if not agency_name:
        return "전국", "전국"

    # 1) 시/도명 직접 포함
    for keyword, sido in REGION_KEYWORDS.items():
        if keyword in agency_name:
            return sido, keyword if keyword not in AMBIGUOUS_NAMES else "전국"

    # 2) ○○시/군/구 추출
    match = re.search(r"([가-힣]{2,}(시|군|구))", agency_name)
    if match:
        sigungu = match.group(1)
        if sigungu in AMBIGUOUS_NAMES:
            return "전국", sigungu
        return REGION_KEYWORDS.get(sigungu, "전국"), sigungu

    # 3) 기관명 전체에서 두 글자 이상 키워드 탐색
    for keyword, sido in REGION_KEYWORDS.items():
        if len(keyword) >= 2 and keyword in agency_name:
            if keyword in AMBIGUOUS_NAMES:
                return "전국", "전국"
            return sido, keyword

    # 4) 기본값
    return "전국", "전국"

# ---------------- merge_and_save ----------------
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

# ---------------- main ----------------
if __name__ == "__main__":
    merge_and_save()
