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
