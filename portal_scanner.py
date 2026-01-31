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

    # Initialize Gemini
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-pro", google_api_key=api_key)

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

    for url in urls:
        print(f"Logistics Officer: Scanning {url}...")
        
        task = f"""
        1. Go to {url}
        2. If you see a Login Screen, stop immediately and return "LOGIN_REQUIRED".
        3. Extract all relevant SCHOOL EVENTS listed on this page.
        4. For each event, look for: Title, Date, Time, Location, Description.
        5. Return the result as a strict JSON list of objects.
        
        Format:
        [
            {{
                "event_title": "Title",
                "start_time": "ISO 8601",
                "end_time": "ISO 8601",
                "location": "Loc",
                "description": "Desc",
                "subjects": ["Tristan", "Benjamin"]
            }}
        ]
        """
        
        try:
            agent = Agent(task=task, llm=llm)
            result = await agent.run()
            final_output = result.final_result()
            
            if "LOGIN_REQUIRED" in final_output:
                print(f"Logistics Officer: Login required for {url}. Please configure credentials.")
                continue

            if "```json" in final_output:
                final_output = final_output.split("```json")[1].split("```")[0].strip()
            elif "```" in final_output:
                final_output = final_output.split("```")[1].split("```")[0].strip()
                
            page_events = json.loads(final_output)
            # Tag source
            for event in page_events:
                event['source'] = 'portal'
            
            all_events.extend(page_events)
            
        except Exception as e:
            print(f"Logistics Officer: Failed scanning {url}: {e}")
            
    # Return deduplicated or raw list
    print(f"Logistics Officer: Found {len(all_events)} total events from Portal.")
    return all_events

if __name__ == "__main__":
    # Test run
    from dotenv import load_dotenv
    load_dotenv()
    print("Running Portal Scanner Test...")
    events = asyncio.run(scan_school_portal())
    print(json.dumps(events, indent=2))
