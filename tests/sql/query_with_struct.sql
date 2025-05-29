select p.playnum        id,
       trim(p.knownas)  name,
       trim(p.inits)    initials,
       trim(p.names)    forenames,
       trim(p.surname)  surname,
       p.dateofbirth    date_of_birth,
       struct(
         c.ctrynum as country_id,
         c.country,
         c.continent
       )                country_of_birth
  from sports.Players p
  left join sports.Countries c
    on p.countryofbirth = c.country
;