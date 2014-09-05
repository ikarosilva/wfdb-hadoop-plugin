function M=shuffle(x,L)
%
% Generates L surrogate time series from x
% by with AmplitudeAdjustedPhaseShuffle
%Underlying assumption is that the Null Hypothesis consists
%of linear dynamics with possibly non-linear, monotonically increasing,
%measurement function.
%
%  1. Amp transform original data to Gaussian distribution
%  2. Phase randomize #1
%  3. Amp transform #2 to original
% Auto-correlation function should be similar but not exact!

x=x(:);
N=x(:);

%Step 1
y=amplitudeTransform(x,N);

%Step 2
y=phaseRand(y);
end

function target=amplitudeTransform(source,N)

target=randn(N,1);

%Steps:
%1. Sort the source by increasing amp
%2. Sort target as #1
%3. Swap source amp by target amp
%4. Sort #3 by increasing time index of #1
X=[[1:N]' x];
X=sortrows(X,2);
target=[X(:,1) sort(target)];
target=sortrows(target,1);
target=target(:,2);

end


function y=phaseShuffle(x)

%%Shuffle spectrum
X=fft(x);
Y=X;
mid=floor(N/2);
phi=2*pi*rand(N/2,1);
Y(1:mid)=abs(X(1:mid))*cos(phi)


%Shuffle the FFT spectrum
for(int i=1;i<midFFT;i++){
    amp = XFFT[i].abs();
    phase =2*Math.PI*rd.nextDouble();
    //Got to make sure that the FFT is symetrical!
    YFFT[i]=new Complex(amp*Math.cos(phase), amp*Math.sin(phase));
    YFFT[NFFT-i]=YFFT[i].conjugate();
    }
    //Reconstruct in the time-domain
    YFFT=FFT.transform(YFFT,TransformType.INVERSE);
    for(int i=0;i<xin.length;i++)
        y[i]=YFFT[i].getReal();
        return y;
        