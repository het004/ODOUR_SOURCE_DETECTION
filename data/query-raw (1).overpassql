[out:json][timeout:60];
{{geocodeArea:Ahmedabad}}->.searchArea;
(
  node["landuse"="industrial"](area.searchArea);
  way["landuse"="industrial"](area.searchArea);

  node["landuse"="landfill"](area.searchArea);
  way["landuse"="landfill"](area.searchArea);

  node["man_made"="wastewater_plant"](area.searchArea);
  way["man_made"="wastewater_plant"](area.searchArea);

  node["amenity"="waste_disposal"](area.searchArea);
  way["amenity"="waste_disposal"](area.searchArea);

  node["amenity"="recycling"](area.searchArea);
  way["amenity"="recycling"](area.searchArea);

  node["amenity"="slaughterhouse"](area.searchArea);
  way["amenity"="slaughterhouse"](area.searchArea);

  node["landuse"="quarry"](area.searchArea);
  way["landuse"="quarry"](area.searchArea);

  node["man_made"="works"](area.searchArea);
  way["man_made"="works"](area.searchArea);

  node["power"="plant"](area.searchArea);
  way["power"="plant"](area.searchArea);

  // Additional tags for broader coverage
  node["landuse"="farmland"](area.searchArea);
  way["landuse"="farmland"](area.searchArea);

  node["landuse"="composting"](area.searchArea);
  way["landuse"="composting"](area.searchArea);

  node["man_made"="refinery"](area.searchArea);
  way["man_made"="refinery"](area.searchArea);

  node["industrial"="chemical"](area.searchArea);
  way["industrial"="chemical"](area.searchArea);

  node["amenity"="incinerator"](area.searchArea);
  way["amenity"="incinerator"](area.searchArea);

  node["man_made"="chimney"](area.searchArea);
  way["man_made"="chimney"](area.searchArea);

  node["man_made"="smokestack"](area.searchArea);
  way["man_made"="smokestack"](area.searchArea);

  node["man_made"="sewage_works"](area.searchArea);
  way["man_made"="sewage_works"](area.searchArea);

  node["amenity"="sewage_treatment_plant"](area.searchArea);
  way["amenity"="sewage_treatment_plant"](area.searchArea);

  // Optional pollution-related transportation nodes
  node["railway"="yard"](area.searchArea);
  way["railway"="yard"](area.searchArea);

  node["aeroway"="airport"](area.searchArea);
  way["aeroway"="airport"](area.searchArea);
);
out body;
>;
out skel qt;
