# 브랜치 규칙

- 새 브랜치 이름은 `{feature|experiment|fix}/{yyyy-mm}/{설명}` 형식으로 만든다. 예: `feature/2026-11/oauth-login`
- `feature`는 새 기능과 추가 기능, `experiment`는 실험적 작업, `fix`는 버그 수정에 쓴다
- `yyyy-mm`은 브랜치를 만드는 시점의 연-월이고 이후 바꾸지 않는다
- 설명은 영어 소문자 kebab-case로 최대 5단어까지 쓴다
- `.claude/hooks/check-branch-name.py` 훅이 규칙에 맞지 않는 브랜치 생성을 차단한다

# 작업 방식

- main에 직접 커밋하지 않는다. 위 규칙에 맞는 기능 브랜치를 만들고 PR로 올린다.
- 커밋은 기능 단위로 작게 나누고(한국어 한 줄로 무엇을 왜), PR은 단계 또는 기능 하나 단위로 묶는다. 커밋마다 PR을 만들지 않는다.
- PR 본문에는 무엇을 왜 바꿨는지, 어떻게 검증했는지를 한국어로 짧게 쓴다.
- merge는 사용자가 직접 한다. Claude는 merge하지 않는다.
- push는 사용자가 요청할 때만 한다.
- 빌드나 테스트가 깨진 상태로 커밋하지 않는다. 실패를 숨기려고 테스트를 지우거나 건너뛰거나 검증을 느슨하게 바꾸지 않는다.
- 설계 선택지가 있으면 구현 전에 선택지와 장단점을 설명하고 사용자가 고른다. 결정은 docs/decisions.md에 기록한다.

# 테스트

- 전체 빌드와 테스트: `./gradlew build` (Docker 필요. Testcontainers로 PostgreSQL, Redis를 띄운다)
- 동시성 실험은 일반 test에서 제외돼 있다: `./gradlew :stock-service:experiment`
- PR마다 `.github/workflows/ci.yml`이 빌드와 테스트를 실행한다. 실험 1은 `.github/workflows/experiment-1.yml`(수동 실행)로 돌린다.
