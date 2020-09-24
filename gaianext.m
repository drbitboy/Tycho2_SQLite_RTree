function row = gaianext(gaiaobj)
  t = gaiaobj.conntcpip;
  row.idoffset = fread(t,1,'int32');
  row.ra = fread(t,1,'float64');
  row.dec = fread(t,1,'float64');
  row.mag = fread(t,1,'float64');

  row.has_light = gaiaobj.has_light;
  row.has_heavy = gaiaobj.has_heavy;

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
  end

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

  end

end
