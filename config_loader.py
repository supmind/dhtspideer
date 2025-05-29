import os
import json
import logging

logger = logging.getLogger(__name__)

def _deep_merge_dicts(base: dict, update: dict) -> dict:
    """
    Recursively merges two dictionaries.
    'update' values take precedence over 'base' values.
    Nested dictionaries are merged as well.
    """
    merged = base.copy()
    for key, value in update.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged

def load_config() -> dict:
    """
    Loads configuration from JSON files based on APP_ENV environment variable.
    It loads a base configuration and merges environment-specific configurations.
    """
    app_env = os.environ.get("APP_ENV", "development").lower()
    logger.info(f"Application environment (APP_ENV): {app_env}")

    valid_envs = ["development", "production"]
    if app_env not in valid_envs:
        logger.warning(
            f"APP_ENV '{app_env}' is not a recognized environment {valid_envs}. "
            f"Falling back to 'development'."
        )
        app_env = "development"

    config_dir = "config"
    base_config_path = os.path.join(config_dir, "base.json")
    env_config_path = os.path.join(config_dir, f"{app_env}.json")

    final_config = {}

    # Load base configuration
    try:
        with open(base_config_path, 'r', encoding='utf-8') as f:
            final_config = json.load(f)
        logger.info(f"Successfully loaded base configuration from '{base_config_path}'.")
    except FileNotFoundError:
        logger.error(f"Base configuration file '{base_config_path}' not found. Starting with an empty config.")
        final_config = {} # Start with empty if base is missing
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from base configuration file '{base_config_path}': {e}. Starting with an empty config.")
        final_config = {} # Start with empty if base is invalid
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading base configuration '{base_config_path}': {e}. Starting with an empty config.")
        final_config = {}


    # Load environment-specific configuration and merge
    env_specific_config = {}
    try:
        with open(env_config_path, 'r', encoding='utf-8') as f:
            env_specific_config = json.load(f)
        logger.info(f"Successfully loaded {app_env} specific configuration from '{env_config_path}'.")
        
        # Deep merge environment config into base config
        final_config = _deep_merge_dicts(final_config, env_specific_config)
        logger.info(f"Successfully merged {app_env} configuration into base configuration.")

    except FileNotFoundError:
        logger.warning(
            f"Environment-specific configuration file '{env_config_path}' not found. "
            f"Proceeding with only base configuration (or previously loaded layers)."
        )
    except json.JSONDecodeError as e:
        logger.warning(
            f"Error decoding JSON from environment-specific file '{env_config_path}': {e}. "
            f"Proceeding with only base configuration (or previously loaded layers)."
        )
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading '{env_config_path}': {e}. "
                     f"Proceeding with only base configuration (or previously loaded layers).")
        
    if not final_config and not env_specific_config: # If both base and env specific failed or were empty
        logger.warning("No configuration loaded. Application might not work as expected.")
        # You might want to return a minimal default structure here if critical
        # For now, returning an empty dict as per earlier logic for missing base.json

    logger.debug(f"Final merged configuration: {final_config}")
    return final_config, app_env # Return both the config and the environment string

if __name__ == '__main__':
    # Example Usage (for testing config_loader.py directly)
    
    # Configure basic logging for the test
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create dummy config files for testing
    if not os.path.exists("config"):
        os.makedirs("config")

    with open("config/base.json", "w") as f:
        json.dump({
            "service_name": "MyServiceBase",
            "logging": {"level": "INFO", "format": "base_format"},
            "common_setting": "base_value",
            "feature_flags": {"feature_a": True, "feature_b": False},
            "nested": {
                "param1": "base1",
                "param2": "base2"
            }
        }, f, indent=2)

    with open("config/development.json", "w") as f:
        json.dump({
            "logging": {"level": "DEBUG"}, # Override logging level
            "database": {"url": "dev_db_url", "timeout": 10},
            "feature_flags": {"feature_b": True, "feature_c": True}, # Override one, add one
             "nested": {
                "param1": "dev1_override", # Override nested
                "param3": "dev3_new"
            }
        }, f, indent=2)

    with open("config/production.json", "w") as f:
        json.dump({
            "service_name": "MyServiceProduction", # Override service name
            "logging": {"format": "prod_format"}, # Override logging format
            "database": {"url": "prod_db_url", "timeout": 30, "retries": 3},
            "feature_flags": {"feature_a": False}, # Override one
            "api_key": "prod_secret_key"
        }, f, indent=2)

    print("--- Testing with APP_ENV=development (default) ---")
    if "APP_ENV" in os.environ:
        del os.environ["APP_ENV"] # Ensure it uses default
    config_dev, env_dev = load_config()
    assert env_dev == "development"
    print(f"Loaded environment: {env_dev}")
    print(json.dumps(config_dev, indent=2))
    # Expected: base merged with development.json

    print("\n--- Testing with APP_ENV=production ---")
    os.environ["APP_ENV"] = "production"
    config_prod, env_prod = load_config()
    assert env_prod == "production"
    print(f"Loaded environment: {env_prod}")
    print(json.dumps(config_prod, indent=2))
    # Expected: base merged with production.json
    
    print("\n--- Testing with APP_ENV=staging (unrecognized) ---")
    os.environ["APP_ENV"] = "staging"
    config_staging, env_staging = load_config() # Should fall back to development
    assert env_staging == "development"
    print(f"Loaded environment: {env_staging}")
    print(json.dumps(config_staging, indent=2))
    # Expected: base merged with development.json (due to fallback)

    print("\n--- Testing with missing environment file (e.g., APP_ENV=test without test.json) ---")
    os.environ["APP_ENV"] = "test"
    if os.path.exists("config/test.json"):
        os.remove("config/test.json")
    config_test_missing, env_test_missing = load_config() # Should use base only, env should be 'test' then fallback
    assert env_test_missing == "development" # Fallback behavior for unrecognized 'test'
    print(f"Loaded environment: {env_test_missing}")
    print(json.dumps(config_test_missing, indent=2))
    # Expected: base.json content only (or base merged with development if 'test' falls back to dev)

    print("\n--- Testing with missing base.json ---")
    # Ensure development.json has some distinct values for this test
    with open("config/development.json", "w") as f_dev_temp:
        json.dump({"source": "development_only", "logging": {"level": "DEBUG"}}, f_dev_temp, indent=2)

    os.rename("config/base.json", "config/base.json.bak")
    os.environ["APP_ENV"] = "development" # back to dev for this test
    config_no_base, env_no_base = load_config()
    assert env_no_base == "development"
    print(f"Loaded environment: {env_no_base}")
    print(json.dumps(config_no_base, indent=2))
    # Expected: development.json content only (as base is now env-specific effectively)
    # Assert a key known to be in development.json
    assert config_no_base.get("source") == "development_only"
    os.rename("config/base.json.bak", "config/base.json") # Restore

    print("\n--- Testing with empty base.json ---")
    with open("config/base.json", "w") as f: json.dump({}, f)
    os.environ["APP_ENV"] = "development"
    config_empty_base, env_empty_base = load_config()
    assert env_empty_base == "development"
    print(f"Loaded environment: {env_empty_base}")
    print(json.dumps(config_empty_base, indent=2))
    # Expected: development.json content only
    # Restore base.json for other potential tests
    with open("config/base.json", "w") as f_base_restore:
        json.dump({
            "service_name": "MyServiceBase",
            "logging": {"level": "INFO", "format": "base_format"},
            "common_setting": "base_value",
            "feature_flags": {"feature_a": True, "feature_b": False},
             "nested": {"param1": "base1", "param2": "base2"}
        }, f_base_restore, indent=2)

    print("\n--- Test with base.json being invalid JSON ---")
    with open("config/base.json", "w") as f_invalid_base: f_invalid_base.write("{invalid_json,}")
    os.environ["APP_ENV"] = "development"
    config_invalid_base, env_invalid_base = load_config()
    assert env_invalid_base == "development"
    print(f"Loaded environment: {env_invalid_base}")
    print(json.dumps(config_invalid_base, indent=2))
    # Expected: development.json content (as base is treated as empty due to error)
    # Restore base.json
    with open("config/base.json", "w") as f_base_restore_2:
        json.dump({
            "service_name": "MyServiceBase",
            "logging": {"level": "INFO", "format": "base_format"},
            "common_setting": "base_value",
            "feature_flags": {"feature_a": True, "feature_b": False},
            "nested": {"param1": "base1", "param2": "base2"}
        }, f_base_restore_2, indent=2)

    print("\n--- Test with development.json being invalid JSON ---")
    # Ensure base.json is valid and has some content for this test
    with open("config/base.json", "w") as f_base_for_invalid_dev_test:
        json.dump({"source": "base_only", "logging": {"level": "INFO"}}, f_base_for_invalid_dev_test, indent=2)

    with open("config/development.json", "w") as f_invalid_dev: f_invalid_dev.write("{invalid_json,}")
    os.environ["APP_ENV"] = "development"
    config_invalid_dev, env_invalid_dev = load_config()
    assert env_invalid_dev == "development"
    print(f"Loaded environment: {env_invalid_dev}")
    print(json.dumps(config_invalid_dev, indent=2))
    # Expected: base.json content only
    assert config_invalid_dev.get("source") == "base_only" # Check base content is present
    # Restore dev.json
    with open("config/development.json", "w") as f_dev_restore:
        json.dump({
            "logging": {"level": "DEBUG"},
            "database": {"url": "dev_db_url", "timeout": 10},
            "feature_flags": {"feature_b": True, "feature_c": True},
            "nested": {"param1": "dev1_override", "param3": "dev3_new"}
        }, f_dev_restore, indent=2)

    # Clean up dummy files (optional, can be left for inspection)
    # os.remove("config/base.json") # These are overwritten by the main task's file creation anyway
    # os.remove("config/development.json")
    # os.remove("config/production.json")
    # os.rmdir("config")
    logger.info("Example usage finished.")
    # Note: The dummy files (base.json, development.json, production.json)
    # will be left in the config directory for the next steps of the main task.
    # They should be populated with actual default values later.
    # For now, base.json is empty as per instructions, others too.
    # The test script creates more comprehensive ones for testing.
    #
    # Actual files for the application:
    # config/base.json: {}
    # config/development.json: {}
    # config/production.json: {}
    # The test script above temporarily overwrites these for its local test run.
    #
    # Let's ensure the actual placeholder files are empty as per initial setup for the main application.
    with open("config/base.json", "w") as f: json.dump({}, f)
    with open("config/development.json", "w") as f: json.dump({}, f)
    with open("config/production.json", "w") as f: json.dump({}, f)
    logger.info("Placeholder config files reset to empty {} for application use.")

```
