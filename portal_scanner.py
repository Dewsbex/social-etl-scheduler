import os
import asyncio
import json
from langchain_google_genai import ChatGoogleGenerativeAI
from browser_use import Agent

async def scan_school_portal():
    """
    Scans the configured School Portal for new events using Browser Use.
    Returns a list of event dictionaries similar to the email extractor.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Logistics Officer: GEMINI_API_KEY not set. Cannot use LLM for portal scan.")
        return []

    # Try multiple models for rotation
    models_to_try = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash", "gemini-1.0-pro"]
    last_err = ""
    
    for model_name in models_to_try:
        try:
            print(f"Logistics Officer: Attempting Portal Scan with {model_name}...")
            llm = ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key)
            agent = Agent(task=task, llm=llm)
            result = await agent.run()
            final_output = result.final_result()
            if final_output:
                break
        except Exception as e:
            last_err = str(e)
            print(f"Logistics Officer: Model {model_name} failed: {last_err}")
            if "429" not in last_err and "quota" not in last_err.lower():
                # If it's not a quota error, it might be a connectivity issue, try next
                continue
            continue
    else:
        print(f"Logistics Officer: All portal scan models failed. Last Error: {last_err}")
        return []

    urls = [
        "https://app.weduc.co.uk/notice/daily/index",
        "https://app.weduc.co.uk/dashboard/newsfeed/list/user/281474978573967",
        "https://app.weduc.co.uk/message/message/index/folder/281474987931452",
        "https://app.weduc.co.uk/message/message/index/folder/281474987931456",
        "https://app.weduc.co.uk/calendar/event/index",
        "https://bishopgilpin.schoolcloud.co.uk/Parent/Home"
    ]
    
    # Allow override via env for flexibility, but default to known user URLs
    env_url = os.getenv("SCHOOL_PORTAL_URL")
    if env_url and env_url not in urls:
        urls.append(env_url)

    all_events = []

    all_urls = ", ".join(urls)
    
    # Prepare auth context
    weduc_user = os.getenv("SCHOOL_USERNAME") or os.getenv("WEDUC_USERNAME")
    weduc_pass = os.getenv("SCHOOL_PASSWORD") or os.getenv("WEDUC_PASSWORD")
    
    auth_step = ""
    if weduc_user and weduc_pass:
        auth_step = f'First, login to app.weduc.co.uk using username "{weduc_user}" and password "{weduc_pass}".'
    else:
        print("Logistics Officer: No portal credentials found in .env")
        return []

    task = f"""
    {auth_step}
    Navigate to and check each of these URLs for school events, schedule changes, or notices:
    {all_urls}
    
    For each page:
    1. Look for new notices, newsfeed items, or calendar events.
    2. Extract: Title, Date/Time, Location, and Description.
    3. Determine if it applies to Tristan (Year 3) or Benjamin (Reception/Year 2).
    
    Return a unified JSON list of all events found. 
    If a date is mentioned without a year, assume 2026. 
    Format dates as ISO 8601 (YYYY-MM-DDTHH:MM:SS).
    
    Format:
    [
        {{
            "event_title": "...",
            "start_time": "...",
            "end_time": "...",
            "location": "...",
            "description": "...",
            "subjects": ["Tristan", "Benjamin"],
            "source_url": "..."
        }}
    ]
    """
    
    try:
        # Extract JSON from potential markdown
        if "```json" in final_output:
            final_output = final_output.split("```json")[1].split("```")[0].strip()
        elif "```" in final_output:
            final_output = final_output.split("```")[1].split("```")[0].strip()
            
        try:
            page_events = json.loads(final_output)
            if not isinstance(page_events, list):
                page_events = []
        except:
            page_events = []

        # Tag source
        for event in page_events:
            event['source'] = 'portal'
        
        all_events = page_events
        print(f"Logistics Officer: Found {len(all_events)} total events from Portal.")
        return all_events
        
    except Exception as e:
        print(f"Logistics Officer: Portal Parsing Failed: {e}")
        return []

if __name__ == "__main__":
    # Test run
    from dotenv import load_dotenv
    load_dotenv()
    print("Running Portal Scanner Test...")
    events = asyncio.run(scan_school_portal())
    print(json.dumps(events, indent=2))
