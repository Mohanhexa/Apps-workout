import yaml
import copy

# Load base config from YAML
with open("C:/Users/Administrator/DoNotDelete/Apps-workout/Spark_application/config_generator/base_config.yaml", "r") as f:
    base_config = yaml.safe_load(f)

# Define environment-specific overrides
env_overrides = {
    "dev": {
        "debug": True,
        "database": {
            "host": "localhost",
            "name": "myapp_dev_db"
        }
    },
    "staging": {
        "debug": False,
        "database": {
            "host": "staging.db.example.com",
            "name": "myapp_staging_db"
        }
    },
    "prod": {
        "debug": False,
        "database": {
            "host": "prod.db.example.com",
            "user": "prod_user",
            "password": "secure_prod_pass",
            "name": "myapp_prod_db"
        }
    }
}

# Function to recursively update base config with overrides
def deep_update(original, updates):
    for key, value in updates.items():
        if isinstance(value, dict) and key in original:
            deep_update(original[key], value)
        else:
            original[key] = value
    return original

# Generate YAML files for each environment
for env, overrides in env_overrides.items():
    env_config = deep_update(copy.deepcopy(base_config), overrides)
    filename = f"{env}_config.yaml"
    with open(filename, "w") as f:
        yaml.dump(env_config, f, default_flow_style=False)
    print(f"Created {filename}")
