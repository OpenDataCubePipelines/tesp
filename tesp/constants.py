"""
Constants
---------
"""

from wagl.constants import ArdProducts


class ProductPackage:
    """
    Helper class for selecting which ard products to package
    """

    _default_excludes = set(
        (ArdProducts.LAMBERTIAN.value.lower(), ArdProducts.SBT.value.lower())
    )

    _all_products = {e.value.lower() for e in ArdProducts}

    @classmethod
    def validate_products(cls, product_list):
        return set(product_list).issubset(cls._all_products)

    @classmethod
    def all(cls):
        return cls._all_products

    @classmethod
    def default(cls):
        return cls._all_products - cls._default_excludes
