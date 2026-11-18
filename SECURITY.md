# Security policy

Do not commit local `config.json`, `.env` files, OAuth tokens, browser cookies,
database files, captures, or generated media. `config.json` and runtime
directories are excluded by `.gitignore`; review `git status` and staged files
before pushing.

Use environment variables or the companion applications' local settings for
credentials. Do not paste secret values into issues or pull requests. Report
security issues privately to the repository owner rather than opening a public
issue with exploit details.
