def _get_dropdown_country_options(page) -> list[str]:
    """
    Open the size-system dropdown and return all available country codes.
    Uses the exact AliExpress classes: comet-v2-menu-item / comet-v2-menu-item-content
    """
    countries = []
    try:
        btn = page.query_selector('button.comet-v2-btn-important')
        if not btn:
            return countries

        btn.click()
        page.wait_for_timeout(1000)

        # Wait for the dropdown body to appear
        page.wait_for_selector('.comet-v2-dropdown-body', timeout=5000)

        option_els = page.query_selector_all(
            '.comet-v2-dropdown-body .comet-v2-menu-item'
        )

        for el in option_els:
            # Text is inside .comet-v2-menu-item-content span
            content = el.query_selector('.comet-v2-menu-item-content')
            text = ((content.inner_text() if content else el.inner_text()) or '').strip().upper()
            if text:
                countries.append(text)  # includes "DEFAULT"

        page.keyboard.press('Escape')
        page.wait_for_timeout(400)

    except Exception as e:
        print(f'[variant_scraper] Dropdown read error: {e}')
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass

    return countries


def _select_country_option(page, country: str) -> bool:
    """
    Open the size-system dropdown and click the matching country item.
    Matches against .comet-v2-menu-item-content text exactly.
    """
    try:
        btn = page.query_selector('button.comet-v2-btn-important')
        if not btn:
            return False

        btn.click()
        page.wait_for_timeout(1000)

        page.wait_for_selector('.comet-v2-dropdown-body', timeout=5000)

        option_els = page.query_selector_all(
            '.comet-v2-dropdown-body .comet-v2-menu-item'
        )

        for el in option_els:
            content = el.query_selector('.comet-v2-menu-item-content')
            text = ((content.inner_text() if content else el.inner_text()) or '').strip().upper()
            if text == country.upper():
                el.click()
                # Wait for size tiles to re-render after country switch
                page.wait_for_timeout(1200)
                return True

        # Option not found — close dropdown
        page.keyboard.press('Escape')
        page.wait_for_timeout(400)
        return False

    except Exception as e:
        print(f'[variant_scraper] Select country "{country}" error: {e}')
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass
        return False


def _scrape_all_country_sizes(page) -> dict:
    """
    Iterate through every country option in the size dropdown,
    scrape the size labels for each, and return a complete size block.
    """
    systems = []

    countries = _get_dropdown_country_options(page)
    print(f'[variant_scraper] Found country options: {countries}')

    if not countries:
        soup    = BeautifulSoup(page.content(), 'html.parser')
        labels  = _scrape_sizes_from_soup(soup)
        country = _detect_country_from_labels(labels) or 'DEFAULT'
        return {
            'type':          'country_mapped' if country != 'DEFAULT' else 'plain',
            'systems':       [{'country': country, 'options': labels}] if labels else [],
            'plain_options': labels if country == 'DEFAULT' else [],
        }

    seen_countries = set()

    for country in countries:
        if country in seen_countries:
            continue
        seen_countries.add(country)

        if country == 'DEFAULT':
            # Select it explicitly so the tiles reset to plain labels
            ok = _select_country_option(page, 'Default')
            if not ok:
                # Already on default — just read
                pass
            page.wait_for_timeout(600)
            soup   = BeautifulSoup(page.content(), 'html.parser')
            labels = _scrape_sizes_from_soup(soup)
            if labels:
                systems.append({'country': 'DEFAULT', 'options': labels})
            continue

        ok = _select_country_option(page, country)
        if not ok:
            print(f'[variant_scraper] Could not select country: {country}')
            continue

        soup   = BeautifulSoup(page.content(), 'html.parser')
        labels = _scrape_sizes_from_soup(soup)
        print(f'[variant_scraper]   {country}: {labels}')

        if labels:
            detected = _detect_country_from_labels(labels)
            systems.append({
                'country': detected or country,
                'options': labels,
            })

    if not systems:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    # Single DEFAULT-only result → treat as plain
    if len(systems) == 1 and systems[0]['country'] == 'DEFAULT':
        return {
            'type':          'plain',
            'systems':       [],
            'plain_options': systems[0]['options'],
        }

    return {
        'type':          'country_mapped',
        'systems':       systems,
        'plain_options': [],
    }
