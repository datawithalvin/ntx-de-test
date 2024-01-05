import httpx
import pandas as pd
import json
from tqdm import tqdm
from bs4 import BeautifulSoup

BASE_URL = "https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}"
levels = [1, 2, 3, 4, 5]
max_pages = [10, 10, 10, 10, 10]
skipped_pages = {}
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

async def fetch_data(level, page):
    async with httpx.AsyncClient(headers=header) as client:
        url = BASE_URL.format(level=level, page=page)
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.content
        except httpx.RequestError as exc:
            print(f"Page {page} for Level {level} skipped due to: {exc}")
            skipped_pages.setdefault(level, []).append(page)
            return None

async def main():
    for level in levels:
        print(f"Scraping Level {level}...")
        
        titles = []
        urls = []
        
        tasks = [fetch_data(level, page) for page in range(1, max_pages[level-1] + 1)]
        responses = await asyncio.gather(*tasks)
        
        for page, content in enumerate(responses, start=1):
            await asyncio.sleep(1)  # Tunda 1 detik
            if content:
                soup = BeautifulSoup(content, 'html.parser')
                
                all_b_tags = soup.find_all("b")
                if len(all_b_tags) > 2:
                    title = all_b_tags[2].text
                    print(f"Page {page}: {title}")
                    titles.append(title)
                else:
                    print(f"Page {page}: No title found.")
                
                div_tag = soup.find("div", onclick=True)
                if div_tag:
                    onclick_value = div_tag.get('onclick')
                    start_index = onclick_value.find("'") + 1
                    end_index = onclick_value.rfind("'")
                    url = onclick_value[start_index:end_index]
                    url_link = f"https://www.fortiguard.com{url}"
                    print(url_link)
                    urls.append(url_link)
                else:
                    print("Attribut onclick tidak ditemukan pada elemen.")
        
        data = pd.DataFrame({'title': titles, 'link': urls})
        data.to_csv(f'datasets/forti_lists_{level}.csv', index=False)
        
    with open('datasets/skipped.json', 'w') as f:
        json.dump(skipped_pages, f)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())