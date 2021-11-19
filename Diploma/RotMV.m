function R=RotMV(w)
% Unitary matrix of rotation R is derived from rotation vector w
d=sqrt(w'*w);
if d==0
R=[1 0 0;0 1 0;0 0 1];
else
c1=w(1,1)/d;
c2=w(2,1)/d;
c3=w(3,1)/d;
M1=[1 0 0;0 1 0;0 0 1];
M2=[c1*c1 c1*c2 c1*c3;c2*c1 c2*c2 c2*c3;c3*c1 c3*c2 c3*c3];
M3=[0 -c3 c2;c3 0 -c1;-c2 c1 0];
R=cos(d)*M1+(1-cos(d))*M2+sin(d)*M3;
end;
