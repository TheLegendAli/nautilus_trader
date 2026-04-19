# Compatibility shim: the compiled base.so was built against the old module name
# 'option_contract'; the actual module is now 'options_contract'.
from nautilus_trader.model.instruments.options_contract import OptionsContract

__all__ = ["OptionsContract"]
