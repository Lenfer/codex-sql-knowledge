# Общая пользовательская установка

Общий runtime: `$HOME/.local/share/sql-knowledge/current/bin/python`. CLI: `$HOME/.local/bin/sql-knowledge`; stdio entrypoint: `$HOME/.local/bin/sql-knowledge-mcp`. Навык находится в `$HOME/.agents/skills/sql-knowledge`. Эти пути не зависят от наличия исходного проекта SQL Knowledge; конкретный release и версии зависимостей записаны рядом с runtime.

CLI fallback в shell:

```sh
"$HOME/.local/bin/sql-knowledge" --project /absolute/project context
"$HOME/.local/bin/sql-knowledge" --project /absolute/project check
```

MCP подключается к одному явно выбранному проекту. В проектном `.codex/config.toml` использовать действительный абсолютный путь к общему Python: TOML не должен полагаться на shell expansion `$HOME` или `~`.

```toml
[mcp_servers.project_knowledge]
command = "/absolute/home/.local/share/sql-knowledge/current/bin/python"
args = ["-m", "sql_knowledge.mcp", "--project", "/absolute/project"]
cwd = "/absolute/project"
startup_timeout_sec = 20
tool_timeout_sec = 120
enabled = true
```

Это шаблон настройки отдельного проекта, не разрешение менять чужую конфигурацию. Не задавать одну глобальную базу для всех проектов и не копировать сервер в каждый проект. Установка навыка не создаёт knowledge.sqlite/knowledge.toml, не выбирает случайный cwd и не переключает старый MCP. Если manifest отсутствует или база legacy, сначала нужна явно разрешённая настройка/миграция. До подключения MCP можно читать существующий совместимый проект через CLI с абсолютным root.

После установки навык доступен со следующего обращения; если Codex не подхватил изменение, перезапустить Codex. Текущий чат не становится автоматически подключённым к новому MCP. Источник формата и skill-discovery: https://learn.chatgpt.com/docs/build-skills .
