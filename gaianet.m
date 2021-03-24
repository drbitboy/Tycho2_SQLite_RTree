 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%% gaianet
 %%%
 %%% - Setup connection to GAIA SQLite server ./gaia/gaialib_server.py
 %%% - Then make query
 %%%
 %%% Usage:
 %%%
 %%% - Host:port is localhost:29073
 %%% - DEC search range is [10.0:12.0]
 %%% - RA search range is [46.0:48.0]
 %%% - Magnitude search is [-Infinity:19.0]
 %%% - negval is -7.0, so struct returned by gaianext (later)
 %%%   will contain additional light and heavy data; valid values are
 %%%   -1.0 (neither), -3.0 (light), -5.0 (heavy), -7.0 (both)
 %%%
 %%% octave> arg.negval = -1.0        %%% Or -3 or -5 or -7
 %%% octave> arg.himag = 19.0         %%% Upper magnitude limit
 %%% octave> arg.ralo = 46.0          %%% Lower RA limit, deg
 %%% octave> arg.rahi = 48.0          %%% Upper RA limit, deg
 %%% octave> arg.declo = 12.0         %%% Lower DEC limit, deg
 %%% octave> arg.dechi = 12.0         %%% Upper DEC limit, deg
 %%% octave> arg.host = 'localhost'   %%% Host where server is running
 %%% octave> arg.port = 29073         %%% Upper DEC limit, deg
 %%%
 %%% octave> go=gaianet(arg)          %%% Connect and make SQL query
 %%%
 %%% Get data, one star per call
 %%%
 %%% octave> nextstar=gaianext(go);   %%% Get first star
 %%% octave> nextstar=gaianext(go);   %%% Get next star
 %%% octave> ...                      %%% until error
 %%%
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

function gaiaobj = gaianet(arg)

  %%% Load defaults, which test gaia_subset DBs
  try negval = arg.negval; catch negval = -1.0;        end
  try himag  = arg.himag;  catch himag  = 19.0;        end
  try ralo   = arg.ralo;   catch ralo   = 46.0;        end
  try rahi   = arg.rahi;   catch rahi   = 48.0;        end
  try declo  = arg.declo;  catch declo  = 10.0;        end
  try dechi  = arg.dechi;  catch dechi  = 12.0;        end
  try host   = arg.host;   catch host   = 'localhost'; end
  try port   = arg.port;   catch port   = 29073;       end

  %%% See internal routine below
  gaiaobj = gaianet_internal(negval,himag,ralo,rahi,declo,dechi,host,port)

end

 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 %%% gaianet_internal
 %%%
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

function gaiaobj = gaianet_internal(negval,himag,ralo,rahi,declo,dechi,host,port)

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
