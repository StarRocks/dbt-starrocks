from setuptools import setup

packages = ["dbt", "dbt.adapters.starrocks", "dbt.include.starrocks"]

package_data = {
    "": ["*"],
    "dbt.include.starrocks": [
        "macros/adapters/*",
        "macros/get_custom_name/*",
        "macros/materializations/seed/*",
        "macros/materializations/snapshot/*",
        "macros/materializations/table/*",
        "macros/materializations/view/*",
    ],
}

install_requires = ["dbt-core", "mysqlclient"]

setup_kwargs = {
    "name": "dbt-starrocks",
    "version": "0.1.0",
    "description": "The starrocks adapter plugin for dbt",
    "long_description": None,
    "author": "long2ice",
    "author_email": "long2ice@gmail.com",
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "python_requires": ">=3.7,<4.0",
}


setup(**setup_kwargs)
