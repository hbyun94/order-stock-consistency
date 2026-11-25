#!/usr/bin/env python3
"""Claude Code PreToolUse hook: 새 브랜치 이름 규칙 검사.

허용 형식: {prefix}/{yyyy-mm}/{kebab-case-설명}
  예) feature/2026-11/oauth-login

- 브랜치를 "새로 만들거나 이름을 바꾸는" git 명령만 검사한다.
  (기존 브랜치에서의 commit / push / checkout 은 건드리지 않는다)
- yyyy-mm 은 브랜치를 만드는 시점의 연-월이어야 한다.
- 규칙에 안 맞으면 exit 2 로 명령을 막고, 이유를 Claude 에게 돌려준다.
"""
import datetime
import json
import re
import shlex
import sys

# ── 설정 ────────────────────────────────────────────────────────────
PREFIXES = ("feature", "experiment", "fix")  # feature 만 쓰려면 ("feature",)
MAX_WORDS = 5                                # 설명 부분 최대 단어 수
# ────────────────────────────────────────────────────────────────────

NAME_RE = re.compile(
    r"^(?P<prefix>%s)/(?P<ym>\d{4}-\d{2})/(?P<desc>[a-z0-9]+(?:-[a-z0-9]+)*)$"
    % "|".join(map(re.escape, PREFIXES))
)
SEPARATORS = {"&&", "||", ";", "|", "&", "(", ")"}
# git 전역 옵션 중 값을 따로 받는 것
GIT_OPTS_WITH_VALUE = {"-C", "-c", "--git-dir", "--work-tree", "--namespace"}
# `git branch` 에서 이 옵션이 있으면 생성이 아니다
BRANCH_NON_CREATE = {
    "-d", "-D", "--delete", "-l", "--list", "-a", "--all", "-r", "--remotes",
    "--show-current", "-v", "-vv", "--verbose", "--merged", "--no-merged",
    "--contains", "--no-contains", "--points-at", "-u", "--set-upstream-to",
    "--unset-upstream", "--edit-description", "--sort", "--format",
}
BRANCH_RENAME = {"-m", "-M", "--move", "-c", "-C", "--copy"}


def split_segments(command):
    """셸 명령을 &&, ;, | 등으로 나눠 토큰 리스트들로 돌려준다."""
    lexer = shlex.shlex(command.replace("\n", " ; "), posix=True,
                        punctuation_chars=True)
    lexer.whitespace_split = True
    segment = []
    for tok in lexer:
        if tok in SEPARATORS:
            if segment:
                yield segment
            segment = []
        else:
            segment.append(tok)
    if segment:
        yield segment


def value_after(args, flags):
    """flags 중 하나 바로 뒤의 값 (또는 --flag=value)."""
    for i, a in enumerate(args):
        if a in flags and i + 1 < len(args):
            return args[i + 1]
        for f in flags:
            if f.startswith("--") and a.startswith(f + "="):
                return a.split("=", 1)[1]
    return None


def new_branch_names(tokens):
    """한 명령 구간에서 새로 만들어지는 브랜치 이름들을 찾는다."""
    if "git" not in tokens:
        return
    args = tokens[tokens.index("git") + 1:]
    # 전역 옵션 건너뛰기
    while args and args[0].startswith("-"):
        args = args[2:] if args[0] in GIT_OPTS_WITH_VALUE else args[1:]
    if not args:
        return
    sub, rest = args[0], args[1:]

    if sub == "checkout":
        name = value_after(rest, {"-b", "-B", "--orphan"})
        if name:
            yield name
    elif sub == "switch":
        name = value_after(rest, {"-c", "-C", "--create", "--force-create",
                                  "--orphan"})
        if name:
            yield name
    elif sub == "worktree" and rest[:1] == ["add"]:
        name = value_after(rest, {"-b", "-B"})
        if name:
            yield name
    elif sub == "branch":
        flags = {a.split("=", 1)[0] for a in rest if a.startswith("-")}
        if flags & BRANCH_NON_CREATE:
            return
        positionals = [a for a in rest if not a.startswith("-")]
        if not positionals:
            return
        if flags & BRANCH_RENAME:
            yield positionals[-1]   # git branch -m [old] new
        else:
            yield positionals[0]    # git branch new [start-point]


def check(name, this_month):
    """문제가 있으면 이유 문자열, 없으면 None."""
    if "$" in name or "`" in name:
        return "변수/치환 없이 브랜치 이름을 그대로 적어주세요"
    m = NAME_RE.match(name)
    if not m:
        return "형식이 맞지 않습니다"
    if m.group("ym") != this_month:
        return "연-월은 브랜치를 만드는 현재 달(%s)이어야 합니다" % this_month
    if len(m.group("desc").split("-")) > MAX_WORDS:
        return "설명은 %d단어 이하로 줄여주세요" % MAX_WORDS
    return None


def main():
    try:
        data = json.load(sys.stdin)
    except ValueError:
        return 0
    if data.get("tool_name") != "Bash":
        return 0
    command = (data.get("tool_input") or {}).get("command") or ""
    if "git" not in command:
        return 0
    try:
        segments = list(split_segments(command))
    except ValueError:      # 따옴표가 안 맞는 등 파싱 불가 → 통과
        return 0

    this_month = datetime.date.today().strftime("%Y-%m")
    for tokens in segments:
        for name in new_branch_names(tokens):
            reason = check(name, this_month)
            if reason:
                sys.stderr.write(
                    "브랜치 이름 규칙 위반: '%s' — %s.\n"
                    "허용 형식: {%s}/%s/{영어-소문자-kebab-case, 최대 %d단어}\n"
                    "예: feature/%s/oauth-login\n"
                    "규칙에 맞는 이름으로 다시 만들어 주세요.\n"
                    % (name, reason, "|".join(PREFIXES), this_month,
                       MAX_WORDS, this_month)
                )
                return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
