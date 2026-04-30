import json
import os
from main import apply_feedback, get_profile, save_profiles, profiles

def test():
    user_id = 12345
    # Симулируем регистрацию / наличие профиля
    if str(user_id) not in profiles:
        profiles[str(user_id)] = {
            "interests": ["python"],
            "interest_weights": {"python": 1.0},
            "query_counts": {},
            "tag_weights": {},
            "language": None,
        }
    
    tags = ["python", "programming", "ai"]
    
    print("Initial profile:", get_profile(user_id))
    
    # Лайкаем
    apply_feedback(user_id, tags, True)
    print("After like:", get_profile(user_id))
    
    # Дизлайкаем
    apply_feedback(user_id, ["programming"], False)
    print("After dislike 'programming':", get_profile(user_id))

if __name__ == "__main__":
    test()
