
.PHONY: check fix test testcov

check:
	@echo "\033[0;34m*** Running Python checks ***\033[0m"
	@echo "\033[1;33mruff\033[0m"
	-@ ruff check || true
	@echo ""
	@echo "\033[1;33mpyright\033[0m"
	-@ pyright || true
	@echo ""

fix:
	@echo "\033[0;34m*** Making Python fixes ***\033[0m"
	@echo "\033[1;33mruff\033[0m"
	-@ruff check --fix || true


test:
	uv run pytest -vv -p no:warnings

testcov:
	uv run coverage run --branch -m pytest tests -vv -p no:warnings \
      --html="tests/pytest_html/test_report.html" --self-contained-html \
			-m "not slow"
	uv run coverage html
	uv run coverage report --fail-under=90
