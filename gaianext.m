 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%% Usage:  set gaianet*.m
 %%% - Summary:
 %%%   octave> G = gaianet(-7.0, himag, loRA,hiRA, loDEC,hiDEC, server);
 %%%   octave> next_star = gaianext(G);
 %%%   octave> next_star = gaianext(G);
 %%%   octave> ...                           %%% until there is an error
 %%%
 %%% See end of this script for contents of returned next_star structure
 %%%
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
function row = gaianext(gaiaobj)

  %%% Read three floats from TCP/IP connection:  code; RA; DEC
  t = gaiaobj.conntcpip;
  row.idoffset = fread(t,1,'int32');
  row.ra = fread(t,1,'float64');
  row.dec = fread(t,1,'float64');
  row.mag = fread(t,1,'float64');

  %%% Use GAIA object to know if light and/or heavy data are present
  row.has_light = gaiaobj.has_light;
  row.has_heavy = gaiaobj.has_heavy;

  %%% Read light data, if present
  if row.has_light
    row.phot_g_mean_mag = row.mag;

    isnulls = fread(t,1,'int32');

    row.parallax = fread(t,1,'float64');
    row.pmra = fread(t,1,'float64');
    row.pmdec = fread(t,1,'float64');
    row.phot_bp_mean_mag = fread(t,1,'float64');
    row.phot_rp_mean_mag = fread(t,1,'float64');

    row.parallax_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.pmra_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.pmdec_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.phot_bp_mean_mag_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.phot_rp_mean_mag_is_null = mod(isnulls,2);

  end   %%% of light data

  %%% Read heavy data, if present
  if row.has_heavy

    isnulls = fread(t,1,'int32');

    row.source_id = fread(t,1,'int64');

    row.ra_error = fread(t,1,'float64');
    row.dec_error = fread(t,1,'float64');
    row.parallax_error = fread(t,1,'float64');
    row.pmra_error = fread(t,1,'float64');
    row.pmdec_error = fread(t,1,'float64');
    row.ra_dec_corr = fread(t,1,'float64');
    row.ra_parallax_corr = fread(t,1,'float64');
    row.ra_pmra_corr = fread(t,1,'float64');
    row.ra_pmdec_corr = fread(t,1,'float64');
    row.dec_parallax_corr = fread(t,1,'float64');
    row.dec_pmra_corr = fread(t,1,'float64');
    row.dec_pmdec_corr = fread(t,1,'float64');
    row.parallax_pmra_corr = fread(t,1,'float64');
    row.parallax_pmdec_corr = fread(t,1,'float64');
    row.pmra_pmdec_corr = fread(t,1,'float64');

    row.ra_error_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.dec_error_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.parallax_error_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.pmra_error_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.pmdec_error_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.ra_dec_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.ra_parallax_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.ra_pmra_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.ra_pmdec_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.dec_parallax_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.dec_pmra_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.dec_pmdec_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.parallax_pmra_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.parallax_pmdec_corr_is_null = mod(isnulls,2);
    isnulls = (isnulls - mod(isnulls,2)) / 2;
    row.pmra_pmdec_corr_is_null = mod(isnulls,2);

  end   %%% of heavy data

end


 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%% Contents of structure returned by gaianext.m
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%%
 %%% octave:4> next_star=gaianext(G);
 %%%
 %%% octave:5> next_star
 %%%
 %%% next_star =
 %%%
 %%%   scalar structure containing the fields:
 %%%
 %%%     idoffset = 3
 %%%     ra =  46.838
 %%%     dec =  11.906
 %%%     mag =  11.722
 %%%     has_light = 1
 %%%     has_heavy = 1
 %%%     phot_g_mean_mag =  11.722
 %%%     parallax =  1.8470
 %%%     pmra =  34.102
 %%%     pmdec = -11.009
 %%%     phot_bp_mean_mag =  10.949
 %%%     phot_rp_mean_mag =  10.949
 %%%     parallax_is_null = 0
 %%%     pmra_is_null = 0
 %%%     pmdec_is_null = 0
 %%%     phot_bp_mean_mag_is_null = 0
 %%%     phot_rp_mean_mag_is_null = 0
 %%%     source_id = 27799815774148480
 %%%     ra_error =  0.031750
 %%%     dec_error =  0.027336
 %%%     parallax_error =  0.035972
 %%%     pmra_error =  0.066830
 %%%     pmdec_error =  0.058633
 %%%     ra_dec_corr =  0.13917
 %%%     ra_parallax_corr =  0.25789
 %%%     ra_pmra_corr = -0.18776
 %%%     ra_pmdec_corr = -0.093095
 %%%     dec_parallax_corr = -0.19574
 %%%     dec_pmra_corr = -0.19396
 %%%     dec_pmdec_corr = -0.24627
 %%%     parallax_pmra_corr =  0.24051
 %%%     parallax_pmdec_corr =  0.21544
 %%%     pmra_pmdec_corr =  0.28363
 %%%     ra_error_is_null = 0
 %%%     dec_error_is_null = 0
 %%%     parallax_error_is_null = 0
 %%%     pmra_error_is_null = 0
 %%%     pmdec_error_is_null = 0
 %%%     ra_dec_corr_is_null = 0
 %%%     ra_parallax_corr_is_null = 0
 %%%     ra_pmra_corr_is_null = 0
 %%%     ra_pmdec_corr_is_null = 0
 %%%     dec_parallax_corr_is_null = 0
 %%%     dec_pmra_corr_is_null = 0
 %%%     dec_pmdec_corr_is_null = 0
 %%%     parallax_pmra_corr_is_null = 0
 %%%     parallax_pmdec_corr_is_null = 0
 %%%     pmra_pmdec_corr_is_null = 0
 %%%
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
