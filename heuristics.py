import re
from datetime import datetime

from state_manager import load_config

def identify_child(text):
    """
    Updated Rule 1: The "Who" Heuristic + Year Group Guardrail.
    """
    text_lower = text.lower()
    
    config = load_config()
    search_settings = config.get("search_settings", {})
    
    # Load settings with fallbacks
    children = search_settings.get("children", ["Benjamin Dewsbery", "Tristan Dewsbery"])
    year_groups = search_settings.get("year_groups", ["Year 3", "Year 5", "Year 6", "Reception Year"])
    clubs = search_settings.get("clubs", ["FOBG", "Friends of Bishop Gilpin", "Krispy Kreme", "Wednesday Notice", "PTA"])
    general_keywords = search_settings.get("general_keywords", ["School Trip", "Assembly", "Sports Day", "Parent Evening", "Costume Day", "donut", "fundraiser"])
    
    # 1. Extraction of Year Groups (Year 1, Y1, etc.)
    years_found = re.findall(r'year\s*(\d)|y(\d)', text_lower)
    extracted_years = [y[0] or y[1] for y in years_found]
    
    # 1b. Parse years from config to check against text
    target_years = []
    for yg in year_groups:
        y_match = re.search(r'(\d)', yg)
        if y_match: target_years.append(y_match.group(1))

    # Helper to check if any item in a list is in text
    def check_keywords(keywords, text):
        return any(k.lower() in text for k in keywords)

    # 2. Check for specific names
    is_dewsbery = "dewsbery" in text_lower
    labels = []

    # Dynamic Child Check from Configuration
    for child_full_name in children:
        name_parts = child_full_name.split()
        first_name = name_parts[0] if name_parts else child_full_name
        
        # Check for any part of the name (e.g. "Tristan" or "Dewsbery")
        # We check full name first
        if child_full_name.lower() in text_lower:
            if first_name not in labels: labels.append(first_name)
            continue
            
        # Check individual parts (longer than 2 chars to avoid 'of', 'jr')
        for part in [p.lower() for p in name_parts if len(p) > 2]:
            # Special case for "Ben" - only match if "Dewsbery" is there too, 
            # otherwise it might be another "Ben"
            if part == "ben" and not is_dewsbery:
                continue
                
            if re.search(fr'\b{re.escape(part)}\b', text_lower):
                if first_name not in labels: labels.append(first_name)
                break
    
    # Check Child Mappings (e.g. Year 2 -> Benjamin)
    child_mappings = config.get("child_mappings", {})
    for child_label, mapped_terms in child_mappings.items():
        if check_keywords(mapped_terms, text_lower):
            if child_label not in labels: labels.append(child_label)
        
    # Check for Year Matches from Configuration
    for y in extracted_years:
        if y in target_years:
            label = f"Year {y}"
            if label not in labels: labels.append(label)

    # Override Keywords (Clubs + General)
    override_keywords = clubs + general_keywords + ["office", "closing", "closed"]
    is_override = check_keywords(override_keywords, text_lower)
    
    is_nursery = "dees days" in text_lower
    
    if is_override:
        if "Bishop Gilpin" not in labels: labels.append("Bishop Gilpin")
            
    # IGNORE - TOKEN PROTECTION
    # If no labels (Tristan, Ben, Configured Year, FOBG, etc.) and not nursery, skip AI call
    if not labels and not is_nursery:
        return "IGNORE"
            
    return labels
        
    # 4. Fallback Labels
    if not labels:
        if is_nursery:
            labels.append("Nursery")
        else:
            # If no years mentioned at all, we show it as Bishop Gilpin
            pass
            
    return labels

def check_gift_heuristic(event_type, description):
    """
    Rule 2: The "Gift" Heuristic
    If Event Type == "Birthday Party", automatically add a reminder 3 days prior: "Buy Gift for [Name]".
    """
    if "Birthday Party" in event_type or "party" in description.lower():
        return True
    return False

def heuristic_extraction(text, subject, msg_id=None):
    """
    Rule 4: Emergency Fallback
    If AI is down, try simple regex extraction for Date/Title.
    """
    # Aggressive HTML Cleaning
    # Remove style and script blocks content completely
    text_clean = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', text, flags=re.IGNORECASE|re.DOTALL)
    # Remove tags
    text_clean = re.sub(r'<[^>]+>', ' ', text_clean)
    # Collapse whitespace
    text_clean = re.sub(r'\s+', ' ', text_clean).strip()
    
    text_full = f"{subject} {text_clean}"
    
    # Construct Gmail URL if valid ID
    gmail_url = f"https://mail.google.com/mail/u/0/#inbox/{msg_id}" if msg_id else None

    # Better date extraction
    months_pattern = "january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec"
    
    # Check for DD Month (e.g., 11 January)
    named_date_match = re.search(fr'(\d{{1,2}})[.\s]+({months_pattern})', text_full, re.IGNORECASE)
    # Check for numerical (e.g., 11/01)
    numerical_date_match = re.search(r'(\d{1,2})[/.-](\d{1,2})([/.-](\d{2,4}))?', text_full)
    
    event_date = None
    
    if named_date_match:
        day = named_date_match.group(1).zfill(2)
        month_str = named_date_match.group(2).lower()[:3]
        months_map = {
            "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
            "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
        }
        month = months_map.get(month_str)
        year = datetime.now().year
        event_date = f"{year}-{month}-{day}"
    elif numerical_date_match:
        d_val = int(numerical_date_match.group(1))
        m_val = int(numerical_date_match.group(2))
        
        # Validation: Day 1-31, Month 1-12. Avoid matching times like 9.45
        if 1 <= d_val <= 31 and 1 <= m_val <= 12:
            day = str(d_val).zfill(2)
            month = str(m_val).zfill(2)
            year = numerical_date_match.group(4) if numerical_date_match.group(4) else datetime.now().year
            if len(str(year)) == 2: year = f"20{year}"
            event_date = f"{year}-{month}-{day}"

    if not event_date:
        return None

    # Try to find a time
    time_match = re.search(r'(\d{1,2})[:.](\d{2})', text_full)
    event_time = "09:00:00"
    if time_match:
        event_time = f"{time_match.group(1).zfill(2)}:{time_match.group(2)}:00"

    labels = identify_child(text_full)
    if labels == "IGNORE": labels = ["Bishop Gilpin"]

    return {
        "event_title": subject,
        "start_time": f"{event_date}T{event_time}",
        "end_time": f"{event_date}T{str(int(event_time[:2])+1).zfill(2)}:00:00",
        "location": "School / TBD",
        # Use simple text_clean here
        "description": f"{(text_clean[:500] + '...') if len(text_clean) > 500 else text_clean}\n\nSource: {gmail_url}",
        "subjects": labels if isinstance(labels, list) else ["Bishop Gilpin"],
        "gmail_url": gmail_url
    }

def check_costume_heuristic(text):
    """
    Rule 3: The "Costume" Protocol
    If keywords "Wear", "Costume", "Dress up" are found -> Flag event as HIGH PRIORITY.
    """
    keywords = ["wear", "costume", "dress up", "fancy dress"]
    text_lower = text.lower()
    for kw in keywords:
        if kw in text_lower:
            return True
    return False
