# KBO Crawler

KBO 홈페이지의 타자 개인 기록과 팀 기록을 수집해 PostgreSQL에 적재하는 크롤러입니다.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

로컬 Unix socket으로 붙는 기본 실행:

```bash
source .venv/bin/activate
python crawler.py
```

TCP로 붙으려면:

```bash
source .venv/bin/activate
python crawler.py --db-host localhost --db-name kbo --db-user anjeonghyeon
```

옵션:

```bash
python crawler.py --help
```

기본적으로 아래 페이지를 수집합니다.

- 선수 타자 기본기록: `https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx?sort=HRA_RT`
- 선수 타자 세부기록: `https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx?sort=HRA_RT`
- 팀 타자 기본기록: `https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx`
- 팀 타자 세부기록: `https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx`
