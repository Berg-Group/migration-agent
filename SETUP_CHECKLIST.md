# Migration Agent Setup Checklist

Use this checklist to ensure both DBT and QA Suite are properly configured.

## ✅ Initial Setup

- [ ] **Clone/Pull Repository**
  ```bash
  git clone <repository-url>
  cd "Migration agent"
  ```

- [ ] **Create .env File**
  ```bash
  cp .env.example .env
  ```
  
- [ ] **Configure .env with Credentials**
  - [ ] Set `REDSHIFT_HOST`
  - [ ] Set `REDSHIFT_PORT` (usually 5439)
  - [ ] Set `REDSHIFT_DATABASE` and `REDSHIFT_DB`
  - [ ] Set `REDSHIFT_USER`
  - [ ] Set `REDSHIFT_PASSWORD`
  - [ ] Set `TARGET_SCHEMA`

## ✅ DBT Setup

- [ ] **Verify Python Installation**
  ```bash
  python3 --version  # Should be 3.6+
  ```

- [ ] **Create Virtual Environment**
  ```bash
  cd DBT
  python3 -m venv redshift_env
  source redshift_env/bin/activate
  ```

- [ ] **Install Python Dependencies**
  ```bash
  pip install -r requirements.txt
  ```

- [ ] **Install dbt Packages**
  ```bash
  dbt deps
  ```

- [ ] **Create dbt_project.yml from Template**
  ```bash
  cp dbt_project.yml.example dbt_project.yml
  ```

- [ ] **Configure dbt_project.yml**
  - [ ] Set `source_database`
  - [ ] Set `clientName`
  - [ ] Set `agency_id`
  - [ ] Set `master_id`
  - [ ] Set `domain`
  - [ ] Set target `schema` for models
  - [ ] Set target `schema` for seeds

- [ ] **Test Redshift Connection**
  ```bash
  set -a && source ../.env && set +a
  python tests/test_redshift_connection.py
  ```

- [ ] **Test dbt Run**
  ```bash
  dbt debug
  dbt run --select example.*
  ```

## ✅ QA Suite Setup

- [ ] **Verify Node.js Installation**
  ```bash
  node --version  # Should be 14+
  npm --version
  ```

- [ ] **Install npm Dependencies**
  ```bash
  cd "QA Suite"
  npm install
  ```

- [ ] **Build TypeScript**
  ```bash
  npm run build
  ```

- [ ] **Test Connection**
  ```bash
  npm run qa
  ```

## ✅ Version Control

- [ ] **Verify .gitignore Files**
  - [ ] Root `.gitignore` exists
  - [ ] `DBT/.gitignore` does NOT ignore `dbt_project.yml`
  - [ ] `QA Suite/.gitignore` ignores `node_modules/` and `logs/`

- [ ] **Check Committed Files**
  ```bash
  git status
  ```
  
  Should be committed:
  - [ ] `dbt_project.yml.example` (template)
  - [ ] `profiles.yml`
  - [ ] `packages.yml`
  - [ ] All SQL models in `models/`
  - [ ] All macros in `macros/`
  - [ ] `package.json` and `package-lock.json`
  - [ ] All TypeScript source files in `src/`
  
  Should NOT be committed:
  - [ ] `.env` file
  - [ ] `DBT/dbt_project.yml` (contains project-specific config)
  - [ ] `DBT/target/` directory
  - [ ] `DBT/dbt_packages/` directory
  - [ ] `DBT/logs/` directory
  - [ ] `DBT/redshift_env/` directory
  - [ ] `QA Suite/node_modules/` directory
  - [ ] `QA Suite/dist/` directory
  - [ ] `QA Suite/logs/` directory

## ✅ Ready to Use

- [ ] **DBT Ready**
  ```bash
  cd DBT
  source redshift_env/bin/activate
  set -a && source ../.env && set +a
  dbt run
  ```

- [ ] **QA Suite Ready**
  ```bash
  cd "QA Suite"
  npm run qa
  ```

## 🚨 Common Issues

### DBT Issues
- **"Could not find profile"**: Load environment variables first
- **"Connection refused"**: Check Redshift security group and VPN
- **"Package not found"**: Run `dbt deps`

### QA Suite Issues  
- **"Cannot find module"**: Run `npm install`
- **"REDSHIFT_DB is not defined"**: Check `.env` file exists in project root
- **"Connection timeout"**: Verify Redshift credentials and network access

## 📝 Notes for Team Members

- Always activate the Python virtual environment before running dbt commands
- The `.env` file must be in the project root (not in subdirectories)
- Load environment variables before each dbt session
- QA Suite logs are stored in `QA Suite/logs/` for review
- Use `npm run qa:quiet` for less verbose output

## 🎯 Quick Start (After Initial Setup)

**Run transformations:**
```bash
cd DBT && source redshift_env/bin/activate && set -a && source ../.env && set +a && dbt run
```

**Run QA tests:**
```bash
cd "QA Suite" && npm run qa
```

## 🔄 Automated Setup

Use the setup script for automated installation:

```bash
./setup.sh
```

This will:
1. Check for `.env` file
2. Set up DBT virtual environment
3. Install all dependencies
4. Test connections
5. Build QA Suite

---

**Last Updated**: {{ current_date }}
**Version**: 1.0

