SYSTEM_PROMPT = "You're a movie critic that watched and analysed thousands of movies."
MESSAGE = """ 
Come up with a dictionary of 50 words that can be used to describe any movie. 
One dictionary word can be a combination of up to 3 basic words. 
Dictionary should be broad, with minimal intersection of meaning, covering as much as possible.
Your goal is to find a way to best describe any possible movie with a dictionary of 50 words.
"""

DICTIONARY = [
"lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", "multi-layered", 
"morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant", "adventure", "violence"
]
TAG_TO_INDEX = {tag: i for i, tag in enumerate(DICTIONARY)}

from typing import List

def to_bitmask(tags: List[str]) -> str:
    bits = ['0'] * len(DICTIONARY)
    for tag in tags:
        i = TAG_TO_INDEX.get(tag)
        if i is not None:
            bits[i] = '1'
    return ''.join(bits)
