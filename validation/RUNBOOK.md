# Colab Validation Runbook

## What This Adds
- `validation/validate_offmarket_colab.py`
  Off-market validator using repo modules plus safe local fixtures. It checks imports, Module 07 readiness semantics, Module 08 off-market health, Module 04 scanner execution, Module 05 strategy input handling, and Module 06 alert formatting with a dry-run Telegram sender.
- `validation/validate_live_dry_run_colab.py`
  Live dry-run validator using the real Module 01 broker login and real Module 02 websocket path. It checks live index resolution, websocket connection, live watchlist seeding, snapshot contract shape, scanner output, and strategy input flow. It does not place orders.
- `validation/RUNBOOK.md`
  This file. It gives the exact Colab execution order and the expected `PASS / FAIL / WARN / SKIP` style outputs.

## Colab Order
1. Mount Drive and open the repo.
```python
from google.colab import drive
drive.mount('/content/drive')
%cd /content/drive/MyDrive/trading_bot
```

2. Install the broker/websocket dependencies used by Modules 01 and 02.
```python
!pip install smartapi-python pyotp
```

3. Run the off-market validator first. This is safe to run any time.
```python
!python validation/validate_offmarket_colab.py --project-root /content/drive/MyDrive/trading_bot
```

4. Set your Angel credentials for the live dry-run.
```python
import os
os.environ["ANGEL_API_KEY"] = "YOUR_API_KEY"
os.environ["ANGEL_CLIENT_CODE"] = "YOUR_CLIENT_CODE"
os.environ["ANGEL_PIN"] = "YOUR_PIN"
os.environ["ANGEL_TOTP_SECRET"] = "YOUR_TOTP_SECRET"
```

5. Run the live dry-run validator during NSE cash market hours only.
Time window: Monday to Friday, 09:15 to 15:30 IST
```python
!python validation/validate_live_dry_run_colab.py --project-root /content/drive/MyDrive/trading_bot --timeout-sec 60
```

## Expected Off-Market Output
- `PASS imports_succeed`
- `PASS workspace_source_loading`
- `PASS maintenance_readiness_consistent`
- `PASS maintenance_readiness_works`
  If this fails, the details will usually tell you which file or mapping contract is still broken in the validation workspace.
- `PASS supervisor_offmarket_health`
- `PASS scanner_executes_offmarket`
- `PASS scanner_generates_candidates_offmarket`
- `PASS strategy_receives_inputs_offmarket`
- `PASS alerts_format_test_output`

## Expected Live Dry-Run Output
- `PASS imports_succeed`
- `PASS credentials_present`
- `PASS broker_login_works`
- `PASS live_index_subscriptions_resolve`
- `PASS live_index_subscription_contract`
- `PASS websocket_connects`
- `PASS live_watchlist_seeding_runs`
- `PASS live_watchlist_subscription_contract`
- `PASS module02_snapshot_contract_appears`
- `PASS module04_produces_candidates`
  This can show `WARN` or `FAIL` if you run it outside market hours or before enough live ticks/candles accumulate.
- `PASS module05_receives_strategy_inputs`
- `PASS no_live_orders_called`

## Normal Warnings
- `WARN market_hours_precondition`
  You ran the live dry-run outside NSE cash market hours. Re-run during market hours for full validation.
- `WARN watchlist_token_unresolved_rows`
  The validator is reporting unresolved rows already present in `watchlist_tokens.json`. It does not invent mappings.

## Safety Notes
- These validators do not call any order placement path.
- The off-market validator uses a dry-run Telegram sender and isolated validation folders under `runtime/validation/`.
- The live dry-run validator uses real broker login and real websocket market data, but still does not place orders.
