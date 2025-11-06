# queuectl

A minimal CLI-based background job queue system built with **Python + Typer + SQLite**.

Submission for: **QueueCTL – Backend Developer Internship Assignment**  
Tech stack: **Python**

---

## 🎯 Overview

`queuectl` is a CLI-based background job processing system. It lets you:

- Enqueue jobs with a **type** and **JSON payload**
- Run a **worker** process that pulls jobs and executes handlers
- Automatically **retry** failed jobs with **exponential backoff**
- Move permanently failed jobs to a **Dead Letter Queue (DLQ)**
- Inspect and **retry DLQ jobs** from the CLI

---

## 🛠 Tech Stack

- **Python 3.8**
- **Typer** – CLI framework
- **SQLite** – persistent storage
- Standard library: `sqlite3`, `datetime`, `json`, `time`

---

## ⚙️ Setup

```bash
git clone https://github.com/<your-username>/queuectl.git
cd queuectl

python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
# source venv/bin/activate

pip install -r requirements.txt
