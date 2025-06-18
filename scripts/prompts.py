SYSTEM_SUMMARY_PROMPT = """You are a strict classification model. You can only respond with words from the dictionary below. 
You are not allowed to use any other words — no variations, no synonyms, no guesses.

Return only a single comma-separated line. No extra text. No formatting. No explanation.

Dictionary = ["lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", 
"multi-layered", "morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant"]"""

USER_SUMMARY_PROMPT = """Summarise the movie "{title}" ({year}) using only the dictionary provided in the system prompt.
You must choose between 5 and 10 words from dictionary. Return only a single comma-separated line. No extra text."""

REFINEMENT_SUMMARY_SYSTEM_PROMPT = """You are a strict language filter. Your job is to repair a given list of descriptive words so that:

1. Only words from the dictionary below are used.  
2. The final result contains 5 to 10 words total.  
3. If any word is not in the dictionary, you must replace it with the **most semantically similar** word from the dictionary.    
4. Return a single comma-separated line with the final words.

Only use this dictionary:

[
"lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", 
"multi-layered", "morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant"
]
"""

REFINEMENT_SUMMARY_USER_PROMPT = """
Original input:
{input}
Return only a single comma-separated line. No extra text.
"""

USER_INPUT_TO_TAGS_SYSTEM_PROMPT = """
You are a strict classification model. You can only respond with words from the dictionary below. 
You are not allowed to use any other words — no variations, no synonyms, no guesses.

Return only a single comma-separated line. No extra text. No formatting.

Dictionary = [
"lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", 
"multi-layered", "morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant"
]
"""

USER_INPUT_TO_TAGS_USER_PROMPT = """
Rephrase the user input into a list of words from the dictionary.

User input:
{input}

Return only a single comma-separated line. No extra text. Use minimun 5 but no more than 10 words.
"""