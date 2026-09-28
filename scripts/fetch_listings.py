"""Fetch live external listings from Cord and split their URLs into chunks.

Writes listings.json (id, company, position, url) and chunks/job_url_part_N.txt,
and prints chunks=[...] to $GITHUB_OUTPUT for the matrix.
"""
import json
import math
import os
import sys

import cord_api

N_CHUNKS = 6
MIN_EXPECTED = 1000  # fewer than this means something went wrong upstream


def main():
    session = cord_api.login()
    rows = cord_api.get_external_listings(session)

    listings = []
    for r in rows:
        url = (r.get("externalURL") or "").strip()
        if not url:
            continue
        listings.append({
            "listing_id": r.get("listingID"),
            "company_id": r.get("companyID"),
            "company_name": r.get("companyName") or "",
            "position": r.get("positionName") or "",
            "url": url,
        })

    if len(listings) < MIN_EXPECTED:
        sys.exit(f"Only {len(listings)} listings came back from Cord; refusing to continue.")

    with open("listings.json", "w", encoding="utf-8") as f:
        json.dump(listings, f)

    urls = list(dict.fromkeys(l["url"] for l in listings))
    print(f"Listings from Cord: {len(listings)} | unique URLs to check: {len(urls)}")

    size = math.ceil(len(urls) / N_CHUNKS)
    os.makedirs("chunks", exist_ok=True)
    idx = []
    for i in range(N_CHUNKS):
        part = urls[i * size:(i + 1) * size]
        if not part:
            continue
        with open(f"chunks/job_url_part_{i + 1}.txt", "w", encoding="utf-8") as out:
            out.write("\n".join(part) + "\n")
        idx.append(i + 1)

    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as gh:
        gh.write(f"chunks={json.dumps(idx)}\n")


if __name__ == "__main__":
    main()
