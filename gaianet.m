function gaiaobj = gaianet(negval,himag,ralo,rahi,declo,dechi,host,port)
  gaiaobj.host = 'localhost';
  gaiaobj.port = 29073;
  gaiaobj.negval = -1;
  gaiaobj.inputs = [-1.0 19.0 46.0 48.0 10.0 12.0];

  if nargin > 0
    gaiaobj.negval = negval;
    gaiaobj.inputs(1,1) = negval;
    if nargin > 5
      gaiaobj.inputs = [negval himag ralo rahi declo dechi];
      if nargin > 6
        gaiaobj.host = varargin{1};
        if nargin > 7
          gaiaobj.port = varargin{2};
        end
      end
    end
  end

  gaiaobj.has_light = (gaiaobj.negval==-3. || gaiaobj.negval==-7.);
  gaiaobj.has_heavy = (gaiaobj.negval==-5. || gaiaobj.negval==-7.);

  gaiaobj.conntcpip = tcpip(gaiaobj.host,gaiaobj.port);
  fopen(gaiaobj.conntcpip);
  fwrite(gaiaobj.conntcpip,gaiaobj.inputs ,'float64');
end
