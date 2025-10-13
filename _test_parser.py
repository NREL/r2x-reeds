from r2x_core.store import DataStore # type: ignore  # noqa: I001
from r2x_reeds import ReEDSConfig, ReEDSGenerator, ReEDSParser, ReEDSDemand, ReEDSEmission
from r2x_reeds.models.components import ReEDSInterface, ReEDSRegion, ReEDSReserve, ReEDSResourceClass, ReEDSTransmissionLine

# Configure
config = ReEDSConfig(
    solve_year=2032,
    weather_year=2012,
    case_name="test_Pacific"
)

# Load data using the default file mapping
file_mapping = ReEDSConfig.get_file_mapping_path()
# print(file_mapping)
data_store = DataStore.from_json(
    file_mapping,
    # folder="/Users/mvelasqu/Documents/marck/GDO/r2x-reeds/tests/data/test_Pacific"
    folder="/Users/mvelasqu/Downloads/test_Pacific"
    # folder="/Users/mvelasqu/Downloads/test_newest_Pacific"
)
data_my = data_store.get_data_file_by_name("modeled_years")
# print(data_my)
data = data_store.read_data_file(name="modeled_years")
# print(data)

# Parse
parser = ReEDSParser(config, data_store)
system = parser.build_system()

# Access components
generators = list(system.get_components(ReEDSGenerator))
loads = list(system.get_components(ReEDSDemand))
emissions = list(system.get_components(ReEDSEmission))
regions = list(system.get_components(ReEDSRegion))
reserves = list(system.get_components(ReEDSReserve))
interfaces = list(system.get_components(ReEDSInterface))
tline = list(system.get_components(ReEDSTransmissionLine))
resource_class = list(system.get_components(ReEDSResourceClass))
print(f"Built system with {len(generators)} generators")
print(f"Built system with {len(loads)} loads")
print(f"Built system with {len(emissions)} emissions")
print(f"Built system with {len(regions)} regions")
print(f"Built system with {len(reserves)} reserves")
print(f"Built system with {len(interfaces)} interfaces")
print(f"Built system with {len(tline)} transmission lines")
print(f"Built system with {len(resource_class)} resource classes")

