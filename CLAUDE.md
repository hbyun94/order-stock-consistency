# 브랜치 규칙

- 새 브랜치 이름은 `{feature|experiment|fix}/{yyyy-mm}/{설명}` 형식으로 만든다. 예: `feature/2026-11/oauth-login`
- `feature`는 새 기능과 추가 기능, `experiment`는 실험적 작업, `fix`는 버그 수정에 쓴다
- `yyyy-mm`은 브랜치를 만드는 시점의 연-월이고 이후 바꾸지 않는다
- 설명은 영어 소문자 kebab-case로 최대 5단어까지 쓴다
- `.claude/hooks/check-branch-name.py` 훅이 규칙에 맞지 않는 브랜치 생성을 차단한다
