def identify_child(text):
    """
    Rule 1: The "Who" Heuristic
    If email mentions "Year 3" -> Assign to Tristan.
    If email mentions "Reception" -> Assign to Benjamin.
    If email mentions "Siblings" -> Assign to Both.
    """
    text_lower = text.lower()
    children = []
    
    if "year 3" in text_lower or "tristan" in text_lower:
        children.append("Tristan")
    
    if "reception" in text_lower or "benjamin" in text_lower or "year 2" in text_lower: # Added Year 2 based on brief, check consistency
        children.append("Benjamin")
        
    if "siblings" in text_lower:
        if "Tristan" not in children: children.append("Tristan")
        if "Benjamin" not in children: children.append("Benjamin")
        
    return children

def check_gift_heuristic(event_type, description):
    """
    Rule 2: The "Gift" Heuristic
    If Event Type == "Birthday Party", automatically add a reminder 3 days prior: "Buy Gift for [Name]".
    """
    if "Birthday Party" in event_type or "party" in description.lower():
        return True
    return False

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
