import requests
import json
import re
import gzip
from tqdm import tqdm
from datetime import datetime
import google.auth
from google.oauth2 import service_account
import google.auth.transport.requests
import os

# ---------------- 기본 설정 ----------------
API_KEY = "ZuK00g5OQwrnp8WTkgktU3rkw62gi5qKb0AkBmz8A16xGhov1WqDbbvOaIx10Sa3kBUqdS9hAEJJ8IS3sTpbgA=="
BASE_URL = "https://api.odcloud.kr/api/gov24/v3"
OUTPUT_PATH = "C:/Users/admin/Documents/GitHub/benefitmatch/benefits.json.gz"
SERVICE_ACCOUNT_FILE = "firebase_service_key.json"  # Firebase 서비스 계정 키 경로
PROJECT_ID = "benefitmatch-bc1ab"  # ★Firebase 콘솔에서 프로젝트 ID 입력

# ---------------- fetch_all_data ----------------
def fetch_all_data(endpoint):
    all_data = []
    page = 1
    per_page = 500
    while True:
        url = f"{BASE_URL}/{endpoint}?page={page}&perPage={per_page}&serviceKey={API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ {endpoint} 오류: {response.status_code}, page={page}")
            print("응답 내용:", response.text)  # ← 추가 (에러 내용 확인)
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
    "수원시": "경기","수원": "경기","성남시": "경기","성남": "경기","고양시": "경기","고양": "경기","용인시": "경기","용인": "경기","부천시": "경기","부천": "경기","안산시": "경기","안산": "경기",
    "안양시": "경기","안양": "경기","남양주시": "경기","남양주": "경기","화성시": "경기","화성": "경기","평택시": "경기","평택": "경기","의정부시": "경기","의정부": "경기","시흥시": "경기","시흥": "경기",
    "파주시": "경기","파주": "경기","김포시": "경기","김포": "경기","광명시": "경기","광명": "경기","군포시": "경기","군포": "경기","이천시": "경기","이천": "경기",
    "오산시": "경기","오산": "경기","양주시": "경기","양주": "경기","구리시": "경기","구리": "경기","안성시": "경기","안성": "경기","포천시": "경기","포천": "경기","의왕시": "경기","의왕": "경기",
    "하남시": "경기","하남": "경기","여주시": "경기","여주": "경기","양평군": "경기","양평": "경기","가평군": "경기","가평": "경기","연천군": "경기","연천": "경기",

    # 강원도
    "춘천시": "강원","춘천": "강원","원주시": "강원","원주": "강원","강릉시": "강원","강릉": "강원","동해시": "강원","동해": "강원","태백시": "강원","태백": "강원","속초시": "강원","속초": "강원",
    "삼척시": "강원","삼척": "강원","홍천군": "강원","홍천": "강원","횡성군": "강원","횡성": "강원","영월군": "강원","영월": "강원","평창군": "강원","평창": "강원","정선군": "강원","정선": "강원",
    "철원군": "강원","철원": "강원","화천군": "강원","화천": "강원","양구군": "강원","양구": "강원","인제군": "강원","인제": "강원","고성군": "강원","고성": "강원","양양군": "강원","양양": "강원","원덕읍": "강원", "원덕": "강원",

    # 충청북도
    "청주시": "충북","청주": "충북","충주시": "충북","충주": "충북","제천시": "충북","제천": "충북","보은군": "충북","보은": "충북","옥천군": "충북","옥천": "충북","영동군": "충북","영동": "충북",
    "진천군": "충북","진천": "충북","괴산군": "충북","괴산": "충북","음성군": "충북","음성": "충북","단양군": "충북","단양": "충북",

    # 충청남도
    "천안시": "충남","천안": "충남","공주시": "충남","공주": "충남","보령시": "충남","보령": "충남","아산시": "충남","아산": "충남","서산시": "충남","서산": "충남","논산시": "충남","논산": "충남",
    "계룡시": "충남","계룡": "충남","당진시": "충남","당진": "충남","금산군": "충남","금산": "충남","부여군": "충남","부여": "충남","서천군": "충남","서천": "충남","청양군": "충남","청양": "충남",
    "홍성군": "충남","홍성": "충남","예산군": "충남","예산": "충남","태안군": "충남","태안": "충남",

    # 전라북도
    "전주시": "전북","전주": "전북","군산시": "전북","군산": "전북","익산시": "전북","익산": "전북","정읍시": "전북","정읍": "전북","남원시": "전북","남원": "전북","김제시": "전북","김제": "전북",
    "완주군": "전북","완주": "전북","진안군": "전북","진안": "전북","무주군": "전북","무주": "전북","장수군": "전북","장수": "전북","임실군": "전북","임실": "전북","순창군": "전북","순창": "전북",
    "고창군": "전북","고창": "전북","부안군": "전북","부안": "전북",

    # 전라남도
    "목포시": "전남","목포": "전남","여수시": "전남","여수": "전남","순천시": "전남","순천": "전남","나주시": "전남","나주": "전남","광양시": "전남","광양": "전남","담양군": "전남","담양": "전남",
    "곡성군": "전남","곡성": "전남","구례군": "전남","구례": "전남","고흥군": "전남","고흥": "전남","보성군": "전남","보성": "전남","화순군": "전남","화순": "전남","장흥군": "전남","장흥": "전남",
    "강진군": "전남","강진": "전남","해남군": "전남","해남": "전남","영암군": "전남","영암": "전남","무안군": "전남","무안": "전남","함평군": "전남","함평": "전남","영광군": "전남","영광": "전남",
    "장성군": "전남","장성": "전남","완도군": "전남","완도": "전남","진도군": "전남","진도": "전남","신안군": "전남","신안": "전남",

    # 경상북도
    "포항시": "경북","포항": "경북","경주시": "경북","경주": "경북","김천시": "경북","김천": "경북","안동시": "경북","안동": "경북","구미시": "경북","구미": "경북","영주시": "경북","영주": "경북",
    "영천시": "경북","영천": "경북","상주시": "경북","상주": "경북","문경시": "경북","문경": "경북","경산시": "경북","경산": "경북","군위군": "경북","군위": "경북","의성군": "경북","의성": "경북",
    "청송군": "경북","청송": "경북","영양군": "경북","영양": "경북","영덕군": "경북","영덕": "경북","청도군": "경북","청도": "경북","고령군": "경북","고령": "경북","성주군": "경북","성주": "경북",
    "칠곡군": "경북","칠곡": "경북","예천군": "경북","예천": "경북","봉화군": "경북","봉화": "경북","울진군": "경북","울진": "경북","울릉군": "경북","울릉": "경북",

    # 경상남도
    "창원시": "경남","창원": "경남","진주시": "경남","진주": "경남","통영시": "경남","통영": "경남","사천시": "경남","사천": "경남","김해시": "경남","김해": "경남","밀양시": "경남","밀양": "경남",
    "거제시": "경남","거제": "경남","양산시": "경남","양산": "경남","의령군": "경남","의령": "경남","함안군": "경남","함안": "경남","창녕군": "경남","창녕": "경남","고성군": "경남","고성": "경남",
    "남해군": "경남","남해": "경남","하동군": "경남","하동": "경남","산청군": "경남","산청": "경남","함양군": "경남","함양": "경남","거창군": "경남","거창": "경남","합천군": "경남","합천": "경남",

    # 제주
    "제주시": "제주","제주": "제주","서귀포시": "제주","서귀포": "제주",

    # 서울
    "강남구": "서울","강남": "서울", "강동구": "서울","강동": "서울", "강북구": "서울","강북": "서울", "강서구": "서울","강서": "서울",
    "관악구": "서울","관악": "서울", "광진구": "서울","광진": "서울", "구로구": "서울","구로": "서울", "금천구": "서울","금천": "서울",
    "노원구": "서울","노원": "서울", "도봉구": "서울","도봉": "서울", "동대문구": "서울","동대문": "서울", "동작구": "서울","동작": "서울",
    "마포구": "서울","마포": "서울", "서대문구": "서울","서대문": "서울", "서초구": "서울","서초": "서울", "성동구": "서울","성동": "서울",
    "성북구": "서울","성북": "서울", "송파구": "서울","송파": "서울", "양천구": "서울","양천": "서울", "영등포구": "서울","영등포": "서울",
    "용산구": "서울","용산": "서울", "은평구": "서울","은평": "서울", "중랑구": "서울","중랑": "서울", "종로구": "서울", "종로": "서울",

    # 광역시 하위 구·군
    "달성군": "대구", "달성": "대구","달서구": "대구", "달서": "대구","수성구": "대구", "수성": "대구",
    "해운대구": "부산", "해운대": "부산","금정구": "부산","금정": "부산", "기장군": "부산","기장": "부산","동래구": "부산","동래": "부산","부산진구": "부산","사상구": "부산","사상": "부산","사하구": "부산","사하": "부산", 
    "수영구": "부산","수영": "부산","연제구": "부산","연제": "부산","영도구": "부산","영도": "부산",
    "광산구": "광주","광산": "광주","대덕구": "대전","대덕": "대전","유성구": "대전","유성": "대전", "울주군": "울산","울주": "울산",
    "미추홀구": "인천", "미추홀": "인천", "연수구": "인천", "연수": "인천", "부평구": "인천", "부평": "인천", "계양구": "인천", "계양": "인천", "강화군": "인천", "강화": "인천", "옹진군": "인천", "옹진": "인천", "남동구": "인천", "남동": "인천",

    # 모호 → 전국 처리 필요
    "중구": "전국","남구": "전국","동구": "전국","서구": "전국","북구": "전국"
}

AMBIGUOUS_NAMES = {"중구", "남구", "동구", "서구", "북구"}

# ---------------- topicMap (앱과 동일하게) ----------------
TOPIC_MAP = {
    '지역 전체 혜택': 'all',
    '서울': 'seoul',
    '부산': 'busan',
    '대구': 'daegu',
    '인천': 'incheon',
    '광주': 'gwangju',
    '대전': 'daejeon',
    '울산': 'ulsan',
    '세종': 'sejong',
    '경기': 'gyeonggi',
    '강원': 'gangwon',
    '충북': 'chungbuk',
    '충남': 'chungnam',
    '전북': 'jeonbuk',
    '전남': 'jeonnam',
    '경북': 'gyeongbuk',
    '경남': 'gyeongnam',
    '제주': 'jeju',
}

def get_topic_name(region_sido):
    if region_sido == "전국":
        return "nation"
    code = TOPIC_MAP.get(region_sido, "all")
    return f"region_{code}"

# ---------------- extract_region ----------------
def extract_region(agency_name: str, phone: str = ""):
    if not agency_name:
        return "전국", "전국"

    # 1) 광주 특별 처리 (전화번호 기반)
    if "광주" in agency_name:
        if phone and phone.startswith("031"):
            return "경기", "광주시"   # 경기도 광주
        elif phone and phone.startswith("062"):
            return "광주", "광주광역시"  # 광주광역시
        # 전화번호 없으면 광주광역시 기본 처리
        return "광주", "광주광역시"

    # 2) 시/도명 직접 포함
    for keyword, sido in REGION_KEYWORDS.items():
        if keyword in agency_name:
            return sido, keyword if keyword not in AMBIGUOUS_NAMES else "전국"

    # 3) ○○시/군/구 추출
    match = re.search(r"([가-힣]{2,}(시|군|구))", agency_name)
    if match:
        sigungu = match.group(1)
        if sigungu in AMBIGUOUS_NAMES:
            return "전국", sigungu
        return REGION_KEYWORDS.get(sigungu, "전국"), sigungu

    # 4) 기관명 전체에서 두 글자 이상 키워드 탐색
    for keyword, sido in REGION_KEYWORDS.items():
        if len(keyword) >= 2 and keyword in agency_name:
            if keyword in AMBIGUOUS_NAMES:
                return "전국", "전국"
            return sido, keyword

    # 5) 기본값
    return "전국", "전국"

# ---------------- detect_region_from_target ----------------
def detect_region_from_target(target_text: str):
    if not target_text:
        return None
    for keyword, sido in REGION_KEYWORDS.items():
        if keyword in target_text:
            return sido
    return None

# ---------------- extract_region_with_target ----------------
def extract_region_with_target(agency_name: str, target_text: str, phone: str = ""):
    region_sido, region_sigungu = extract_region(agency_name, phone)
    if region_sido == "전국" or not region_sido:
        target_region = detect_region_from_target(target_text)
        if target_region:
            region_sido = target_region
    return region_sido, region_sigungu

# ---------------- merge_and_save ----------------
def merge_and_save():
    print("✅ 서비스 목록 불러오는 중...")
    service_list = fetch_all_data("serviceList")
    print(f"👉 총 {len(service_list)}건 불러옴")

    print("✅ 상세 내용 불러오는 중...")
    detail_list = fetch_all_data("serviceDetail")

    print("✅ 병합 중...")
    detail_map = {d["서비스ID"]: d for d in detail_list if "서비스ID" in d}
    condition_map = {}

    merged = []
    for s in tqdm(service_list):
        sid = s.get("서비스ID", "")
        detail = detail_map.get(sid, {})
        condition = condition_map.get(sid, {})

        region_sido, region_sigungu = extract_region_with_target(
            s.get("소관기관명", ""),
            s.get("지원대상", ""),
            s.get("전화문의", "")
        )

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

    with gzip.open(OUTPUT_PATH, "wt", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)

    print(f"🎉 총 {len(merged)}건 저장 완료 → {OUTPUT_PATH} (gzip 압축)")
    return merged

def load_existing_ids():
    """기존 benefits.json.gz에서 id 목록 불러오기"""
    if not os.path.exists(OUTPUT_PATH):
        return set()
    with gzip.open(OUTPUT_PATH, "rt", encoding="utf-8") as f:
        data = json.load(f)
    return {b["id"] for b in data}
# ---------------- Firebase Access Token ----------------
def get_access_token():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/firebase.messaging"],
    )
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

# ---------------- Firebase HTTP 알림 ----------------
def send_fcm_http_v1(topic, title, body):
    access_token = get_access_token()
    url = f"https://fcm.googleapis.com/v1/projects/{PROJECT_ID}/messages:send"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; UTF-8",
    }
    payload = {
        "message": {
            "topic": topic,
            "notification": {
                "title": title,
                "body": body
            }
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    print(f"FCM HTTP 응답({topic}): {response.status_code} {response.text}")

# ---------------- 신규 혜택 처리 ----------------
def is_same_day(date_str):
    """YYYYMMDD[HHMMSS] 형식 앞 8자리로 오늘 여부 판단"""
    if not date_str:
        return False
    try:
        return str(date_str)[:8] == datetime.now().strftime("%Y%m%d")
    except Exception as e:
        print(f"날짜 파싱 실패: {date_str}, {e}")
        return False

def notify_new_benefits(benefits):
    # 1) 지역별(전국 제외) 시·도 집합
    region_sidos = sorted({
        b.get("regionSido")
        for b in benefits
        if b.get("regionSido") and b.get("regionSido") != "전국"
    })

    # 2) 지역별 각 1회 (제목에 시·도명 표시)
    for sido in region_sidos:
        topic = get_topic_name(sido)  # ex) region_busan
        send_fcm_http_v1(topic, f"{sido} 신규 혜택이 등록됐어요!", "지금 확인해보세요.")

    # 3) 지역 신규가 1개라도 있으면 region_all 1회 (라벨 명확화)
    if region_sidos:
        send_fcm_http_v1("region_all", "전체 지역 신규 혜택이 등록됐어요!", "지금 확인해보세요.")

    # 4) 전국 운영 혜택이 있으면 nation 1회 (라벨 명확화)
    if any(b.get("regionSido") == "전국" for b in benefits):
        send_fcm_http_v1("nation", "전국 운영 혜택이 등록됐어요!", "지금 확인해보세요.")

# ---------------- main ----------------
if __name__ == "__main__":
    existing_ids = load_existing_ids()   # 이전 파일의 ID들
    is_bootstrap = (len(existing_ids) == 0)   # ✅ 추가: 초기 실행 여부 표시

    new_data = merge_and_save()          # 새로 크롤링/병합된 전체 데이터

    # 1) 파일 기준 '처음 발견된' 신규 (등록일과 무관)
    newly_discovered = [b for b in new_data if b["id"] not in existing_ids]

    # ✅ 추가: 초기 실행이면 '처음 발견' 제외해 대량 건수 방지
    if is_bootstrap:
        print("초기 실행 감지 → 오늘 등록분만 알림(처음발견 제외)")
        newly_discovered = []

    # 2) 오늘 '등록'된 항목 (이미 있던 것도 포함)
    today_registered = [b for b in new_data if is_same_day(b.get("registerDate", ""))]

    # 3) 알림 후보: (처음 발견) ∪ (오늘 등록)
    candidates_by_id = {}
    for b in newly_discovered + today_registered:
        candidates_by_id[b["id"]] = b
    candidates = list(candidates_by_id.values())

    # 디버그 로그
    print(
        f"처음발견:{len(newly_discovered)} / 오늘등록:{len(today_registered)} "
        f"→ 알림대상 합계:{len(candidates)}"
    )
    if newly_discovered[:5]:
        print("샘플 처음발견 ID:", [b["id"] for b in newly_discovered[:5]])

    if candidates:
        print(f"알림 대상 {len(candidates)}건 → FCM 전송")
        notify_new_benefits(candidates)
    else:
        print("신규/갱신 없음 → 알림 생략")