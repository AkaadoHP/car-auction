# Car Auctions Data Pipeline

This project collects and organizes car auction data (Copart, IAAI, [auction-data.info](https://auction-data.info)) for analysis.

---

## 🚀 Features
- Fetch upcoming auctions via **auction-data.info** API.
- Store auctions in **PostgreSQL** (timestamps in UTC).
- Query auctions starting in the next **12h / 24h**.
- (Planned) Add **Redis caching** for API efficiency.
- (Planned) Extend to Copart + IAAI integration.
