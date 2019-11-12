"""
Constants
---------
"""

from wagl.constants import ArdProducts
from wagl.constants import AtmosphericCoefficients


class ProductPackage:
    """
    Helper class for selecting which ard products to package
    """
    _default_excludes = set((
        ArdProducts.LAMBERTIAN.value.lower(),
        ArdProducts.SBT.value.lower(),
        ArdProducts.ADJ.value.lower(),
        ArdProducts.SKY.value.lower()
    ))

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

    @classmethod
    def marine(cls):
        marine_products = cls._all_products - {ArdProducts.SBT.value.lower()}
        marine_products.add(AtmosphericCoefficients.FS.value.lower())
        return marine_products
