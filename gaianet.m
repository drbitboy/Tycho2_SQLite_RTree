 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%% Usage:
 %%%
 %%% Setup connection, make query
 %%% - Host:port is localhost:29073
 %%% - DEC search range is [10.0:12.0]
 %%% - RA search range is [46.0:48.0]
 %%% - Magnitude search is [-Infinity:19.0]
 %%% - negval is -7.0, so struct returned by gaianext (later)
 %%%   will contain additional light and heavy data; valid values are
 %%%   -1.0 (neither), -3.0 (light), -5.0 (heavy), -7.0 (both)
 %%%
 %%% octave> go=gaianet(-7.0, 19.0, 46.0, 48.0, 10.0, 12.0, 'localhost', 29073);
 %%%
 %%% Get data, one star per call
 %%%
 %%% octave> nextstar=gaianext(go);
 %%% octave> nextstar=gaianext(go);
 %%% octave> ... %%% until error
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

function gaiaobj = gaianet(negval,himag,ralo,rahi,declo,dechi,host,port)

  %%% Setup defaults
  gaiaobj.host = 'localhost';
  gaiaobj.port = 29073;
  gaiaobj.negval = -1;
  gaiaobj.inputs = [-1.0 19.0 46.0 48.0 10.0 12.0];

  %%% Override defaults with arguments
  if nargin > 0
    gaiaobj.negval = negval;
    gaiaobj.inputs(1,1) = negval;
    if nargin > 5
      gaiaobj.inputs = [negval himag ralo rahi declo dechi];
      if nargin > 6
        gaiaobj.host = host;
        if nargin > 7
          gaiaobj.port = port;
        end
      end
    end
  end

  %%% Parse negval (-1.0
  gaiaobj.has_light = (gaiaobj.negval==-3. || gaiaobj.negval==-7.);
  gaiaobj.has_heavy = (gaiaobj.negval==-5. || gaiaobj.negval==-7.);

  %%% Open TCP/IP socket
  gaiaobj.conntcpip = tcpip(gaiaobj.host,gaiaobj.port);
  fopen(gaiaobj.conntcpip);

  %%% Write request
  fwrite(gaiaobj.conntcpip,gaiaobj.inputs ,'float64');

  %%% Returns gaia object, suitable for use with gaianext.m
end
