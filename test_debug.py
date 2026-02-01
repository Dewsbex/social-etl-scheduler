import re
import datetime
from heuristics import heuristic_extraction

subject = "Training at Goals, Stade de France pitch, 9.45 for 10.00"
text = """Spond
	A kind reminder to respond to my invite.
Andrew Mortimer in the group Wimbledon RFC U7 mixed rugby 25-26 season
Training at Goals, Stade de France pitch, 9.45 for 10.00

Sunday 11. January at 10:00

Meeting at 09:45

Goals Wimbledon, Beverley Way, London

With a risk of frozen or flooded pitches, we are moving training to Goals, off the A3. Please note that the club doesn't cover the cost of this, instead the coaching group are paying. If you would like to make a voluntary contribution of £5 on the day please speak to Andrew or Will Jagger.

Kids will need to wear trainers or astro boots, studded boats are not allowed on the pitches at Goals.

Can you hit 'Attending' or 'Decline' to let us know if you can make training this Sunday at Goals.

Will Benji Dewsbery attend?"""

print("Testing heuristic extraction...")
result = heuristic_extraction(text, subject, "test_id")
print(result)
