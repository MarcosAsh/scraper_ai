#!/usr/bin/env python3
"""
Simple web scraper using requests and BeautifulSoup with robots.txt compliance,
rate limiting, and text extraction.
"""
import time
import argparse
import logging
import re
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

# Customize your User-Agent
HEADERS = {"User-Agent": "MyLMTrainer/1.0 (+https://yourdomain.com)"}

def fetch_url(url):
    """Fetch the raw HTML for a given URL."""
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.text

def normalize_text(text):
    """Lowercase, collapse whitespace, and strip surrounding spaces."""
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_text(html):
    """Extract and clean text from HTML, removing scripts, styles, and boilerplate."""
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'nav', 'footer', 'aside', 'form']):
        tag.decompose()
    paragraphs = [p.get_text(separator=' ', strip=True) for p in soup.find_all('p')]
    content = '\n'.join(p for p in paragraphs if len(p) > 50)
    return normalize_text(content)

def allowed_by_robots(url, robots_parsers):
    """Check robots.txt for permission to crawl the URL."""
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    rp = robots_parsers.get(base)
    if not rp:
        robots_url = urljoin(base, '/robots.txt')
        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception as e:
            logging.warning("Could not read robots.txt at %s: %s", robots_url, e)
        robots_parsers[base] = rp
    return rp.can_fetch(HEADERS['User-Agent'], url)

def crawl(start_urls, max_pages=100, delay=1.0, output_file='corpus.txt', max_tokens=None, allowed_domains=None):
    """Crawl pages starting from start_urls up to max_pages or max_tokens, respecting robots.txt and rate limits.
    Only follow links within allowed_domains if provided."""
    seen = set()
    queue = list(start_urls)
    robots_parsers = {}
    # Determine allowed domains: use provided or derive from seed URLs
    if allowed_domains is None:
        allowed_domains = set(urlparse(u).netloc for u in start_urls)
    else:
        allowed_domains = set(allowed_domains)
    # Initialize token counter
    token_count = 0
    with open(output_file, 'a', encoding='utf-8') as out:
        while queue and len(seen) < max_pages and (max_tokens is None or token_count < max_tokens):
            url = queue.pop(0)
            if url in seen:
                continue
            if not allowed_by_robots(url, robots_parsers):
                logging.info("Disallowed by robots.txt: %s", url)
                seen.add(url)
                continue
            logging.info("Fetching (%d/%d): %s", len(seen) + 1, max_pages, url)
            try:
                html = fetch_url(url)
            except Exception as e:
                logging.error("Failed to fetch %s: %s", url, e)
                seen.add(url)
                time.sleep(delay)
                continue
            text = extract_text(html)
            if text:
                out.write(text + "\n\n")
                # Update token count if limit is set
                if max_tokens is not None:
                    tokens = len(text.split())
                    token_count += tokens
                    logging.info("Extracted %d tokens, total %d/%d", tokens, token_count, max_tokens)
                    if token_count >= max_tokens:
                        logging.info("Reached max tokens limit (%d). Stopping crawl.", max_tokens)
                        break
            seen.add(url)
            parsed = urlparse(url)
            base_netloc = parsed.netloc
            soup = BeautifulSoup(html, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                full = urljoin(url, href)
                parsed_full = urlparse(full)
                # Only follow http(s) links within allowed domains
                if parsed_full.scheme not in ('http', 'https'):
                    continue
                if parsed_full.netloc not in allowed_domains:
                    continue
                if full not in seen and full not in queue:
                    queue.append(full)
            time.sleep(delay)

def main():
    parser = argparse.ArgumentParser(description='Simple web scraper with optional token and domain limits')
    parser.add_argument('seeds', nargs='+', help='Seed URLs to start crawling from')
    parser.add_argument('--max-pages', type=int, default=100,
                        help='Maximum number of pages to crawl')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between requests in seconds')
    parser.add_argument('--output', default='corpus.txt',
                        help='Output file path (appended)')
    parser.add_argument('--max-tokens', type=int, default=None,
                        help='Maximum number of tokens to crawl')
    parser.add_argument('--allowed-domains', type=str, default=None,
                        help='Comma-separated list of domain names to restrict crawling to (includes seed domains by default)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')
    # Parse allowed domains if provided
    allowed = None
    if args.allowed_domains:
        allowed = [d.strip() for d in args.allowed_domains.split(',') if d.strip()]
    crawl(args.seeds, args.max_pages, args.delay, args.output,
          max_tokens=args.max_tokens, allowed_domains=allowed)

if __name__ == '__main__':  # pragma: no cover
    main()