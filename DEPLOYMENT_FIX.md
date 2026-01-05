# 🔧 File Ownership Fix - Deployment Guide

## Problem Summary

Docker containers were running as UID `50000` (Airflow user), causing file ownership conflicts:
- Files created by Docker were owned by UID `50000`
- Git commands failed with "dubious ownership" errors
- CI/CD pipeline couldn't manage files properly

## Solution

**Run Docker containers as your user (`mouad`, UID `1000`) instead of UID `50000`**

This eliminates all permission conflicts:
- ✅ Files owned by you
- ✅ Git works seamlessly
- ✅ No permission juggling needed
- ✅ Simpler CI/CD pipeline

---

## 🚀 Deployment Steps

### Step 1: Fix Current Ownership (One-Time)

On your **Raspberry Pi**, run:

```bash
cd ~/dataops/airflow-self-hosted
bash fix_ownership_once.sh
```

This script will:
1. Stop Docker containers
2. Change all files from UID `50000` to your user
3. Configure git safe directory
4. Restart containers with new configuration

### Step 2: Verify Everything Works

```bash
# Check container status
docker compose ps

# Verify file ownership
ls -la ~/dataops/airflow-self-hosted

# Test git
git status
```

### Step 3: Deploy from GitHub

Commit and push the changes:

```bash
git add .
git commit -m "fix: use host user UID instead of 50000 for Docker containers"
git push origin main
```

The CI/CD pipeline will now work without permission issues!

---

## 📝 What Changed

### 1. Docker Compose (`docker-compose.yml`)
```yaml
# Before:
user: "${AIRFLOW_UID:-50000}:0"

# After:
user: "${AIRFLOW_UID:-1000}:0"
```

### 2. GitHub Actions Workflow (`.github/workflows/deploy_airflow.yml`)
```bash
# Before:
echo "AIRFLOW_UID=50000" > .env

# After:
echo "AIRFLOW_UID=1000" > .env
```

### 3. Removed Permission Management
- ❌ No more `chown` commands in deployment scripts
- ❌ No more sudo password passing
- ❌ No more permission resets
- ✅ Simple, clean deployments

---

## 🎯 Benefits

1. **Simpler Architecture**: One user for everything
2. **No Permission Conflicts**: Files always owned by you
3. **Git Always Works**: No "dubious ownership" errors
4. **Cleaner CI/CD**: No complex permission management
5. **Easier Debugging**: Standard file permissions

---

## 🔍 Verification

After deployment, verify on Raspberry Pi:

```bash
# Check file ownership (should be mouad:mouad or your username)
ls -la ~/dataops/airflow-self-hosted

# Check Docker user (should show UID 1000)
docker compose exec airflow-apiserver id

# Test git
cd ~/dataops/airflow-self-hosted
git status

# Check Airflow
docker compose ps
docker compose logs airflow-apiserver --tail=50
```

---

## 🆘 Troubleshooting

### If containers don't start:

```bash
# Check logs
docker compose logs

# Verify .env file
cat ~/dataops/airflow-self-hosted/.env | grep AIRFLOW_UID

# Should show: AIRFLOW_UID=1000
```

### If you get permission errors:

```bash
# Re-run the fix script
cd ~/dataops/airflow-self-hosted
bash fix_ownership_once.sh
```

### If git still complains:

```bash
# Add safe directory
git config --global --add safe.directory ~/dataops/airflow-self-hosted
```

---

## 📚 Technical Details

### User ID Mapping

- **Host User**: `mouad` (UID 1000, GID 1000)
- **Docker User**: UID 1000, GID 0 (root group for Airflow compatibility)
- **Result**: Files created by Docker are owned by `mouad`

### Why GID 0 (root group)?

Airflow requires certain permissions that are easier to manage with GID 0. This is safe because:
- The user is still non-root (UID 1000)
- Only group membership is root
- Standard practice in Airflow Docker deployments

---

## ✅ Success Criteria

You'll know it's working when:

1. ✅ `ls -la` shows files owned by `mouad:mouad` (or `mouad:root`)
2. ✅ `git status` works without errors
3. ✅ Airflow UI is accessible and functional
4. ✅ DAGs are loading and running
5. ✅ CI/CD deployments complete without permission errors

---

## 🎉 Next Steps

After successful deployment:

1. Monitor the first few automated deployments
2. Verify DAGs are running correctly
3. Check logs for any permission-related warnings
4. Delete the temporary fix script: `rm fix_ownership_once.sh`

---

**Note**: UID 1000 is the standard first user ID on Linux systems. If your user has a different UID, update the workflow accordingly by running `id -u` on your Raspberry Pi.

