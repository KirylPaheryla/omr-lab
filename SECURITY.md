# Security Policy – OMR Lab (Master’s Thesis)

OMR Lab is a research/academic project. Security handling is **best-effort** by a single maintainer.

## Supported Versions
We actively support the latest `main` branch only.

| Version | Supported |
|--------:|:---------:|
| main    | ✅        |
| tags    | ❌ (no backports) |

## Reporting a Vulnerability
**Do not disclose publicly.** Use one of the private channels:

1. **GitHub Private Vulnerability Report (preferred):**  
   Go to **Security → Report a vulnerability** in this repository.
2. If the above is unavailable, open a **blank issue** with the title “Security contact request” (no details), and we’ll move to a private channel.

Please include (when possible):
- Affected commit/tag, OS, Python/Poetry versions
- Minimal PoC / steps to reproduce, logs, expected vs actual behavior
- Impact (e.g., RCE, DoS, info leak), suggested severity
- Your contact and whether you want public credit

## Disclosure Policy (Best-Effort)
- Acknowledge within **7 days**
- Triage within **14 days**
- Target fix: **≤30 days** for Critical/High, **≤90 days** for others  
  (Timeframes are goals; as a thesis project, delays are possible.)

We’ll publish a GitHub Security Advisory with affected versions and remediation guidance. CVE assignment is not guaranteed.

## Scope
In scope:
- Code and configs in **this repository** (Python CLI, data tooling, pipelines)

Out of scope:
- **Third-party tools/binaries** (e.g., MuseScore, Verovio)
- **External datasets/content** and their licenses
- OS packages, drivers, GPU/accelerator software

## Responsible Testing Rules
- No destructive testing against third-party services/infrastructure
- Keep PoCs minimal and local
- Do not exfiltrate data beyond what is needed to demonstrate the issue

## Secure Usage Notes (Users)
- Treat all **input files** (images, MusicXML/MXL) as **untrusted**
- Run in a **virtual environment** or sandbox; prefer least privileges
- External renderers (MuseScore/Verovio) are executed as **child processes**; verify their paths and integrity
- Avoid processing files from unknown sources; disable network if not required

## Supply Chain & Dependencies
- Dependencies are pinned via **Poetry lockfile**
- We use linters/tests and may enable Dependabot for alerts
- If you suspect typosquatting or a malicious dependency, please report privately

## Remediation & Releases
- Fixes land on `main` (no maintained release branches)
- Advisories include affected commits/tags and suggested mitigations
- Temporary workarounds may be provided if a full fix needs more time

## Credits
We credit reporters in advisories by default (unless anonymity is requested).
