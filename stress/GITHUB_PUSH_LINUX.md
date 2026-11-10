# Push from a Linux lab PC (HTTPS or SSH)

Repo: `https://github.com/reffobic/RescueMesh`  
Branch: `stress-testing-lab`

```bash
cd ~/Projects/RescueMesh
# or: cd ~/Group202/RescueMesh
```

Do **not** commit `stress/results/` (test output). Prefer:

```bash
git add stress/report stress/PHASE1_REPORT_GUIDE.md stress/MULTI_PC_LAB_GUIDE.md stress/README.md
git status
git commit -m "Add Phase 1 stress suite and guides"
```

---

## Method A — HTTPS + Personal Access Token (simplest)

1. Create a token: GitHub → **Settings** → **Developer settings** → **Personal access tokens** → classic token with **`repo`** scope. Copy it once.

2. Point remote at HTTPS and push:

```bash
git remote set-url origin https://github.com/reffobic/RescueMesh.git
git remote -v

git checkout stress-testing-lab
git pull origin stress-testing-lab

git push -u origin stress-testing-lab
```

When prompted:
- **Username:** your GitHub username  
- **Password:** paste the **token** (not your GitHub password)

Optional — remember credentials:

```bash
git config --global credential.helper store
git push -u origin stress-testing-lab
```

---

## Method B — SSH

```bash
# 1) Key (skip if you already have one)
ls ~/.ssh/id_ed25519.pub || ssh-keygen -t ed25519 -C "lab-$(hostname)" -N "" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub
```

2. GitHub → **Settings** → **SSH and GPG keys** → **New SSH key** → paste the `ssh-ed25519 ...` line.

Or:

```bash
gh auth login
gh ssh-key add ~/.ssh/id_ed25519.pub --title "lab-$(hostname)"
```

```bash
# 3) Test
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
ssh -T git@github.com
# expect: Hi <user>! You've successfully authenticated...

# 4) Remote + push
git remote set-url origin git@github.com:reffobic/RescueMesh.git
git remote -v

git checkout stress-testing-lab
git pull origin stress-testing-lab
git push -u origin stress-testing-lab
```

---

## After a successful push — other lab PCs

```bash
cd ~/Projects/RescueMesh
git fetch origin
git checkout stress-testing-lab
git pull origin stress-testing-lab
```

---

## If both methods fail with 403 / Permission denied

Your GitHub user cannot write to `reffobic/RescueMesh`. Ask the repo owner to add you as a **collaborator**, or push to your own **fork** instead.
