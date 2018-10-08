"""
Constants
---------
"""

from wagl.constants import ArdProducts


class ProductPackage(object):
    """
    Helper class for selecting which ard products to package
    """
    _default_excludes = set((
        ArdProducts.LAMBERTIAN.value,
        ArdProducts.SBT.value
    ))

    _all_products = set([e.value for e in ArdProducts])

    @classmethod
    def validate_products(cls, product_list):
        return set(product_list).issubset(cls._all_products)

    @classmethod
    def all(cls):
        return cls._all_products

    @classmethod
    def default(cls):
        return cls._all_products - cls._default_excludes
